# conexao_peer.py
from __future__ import annotations

import json
import socket
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from estado import EstadoPeerLocal
from tabela_peers import TabelaPeers


class ConexaoPeer:
    """
    Representa uma conexão TCP PERSISTENTE com um único peer.

    Responsabilidades:
      - Abrir o socket e fazer o HELLO/HELLO_OK.
      - Manter um loop de leitura em thread separada.
      - Encerrar a conexão de forma graciosa (BYE / BYE_OK no futuro).
      - Atualizar a TabelaPeers com estado e RTT.
      - Notificar um callback opcional (on_mensagem) ao receber mensagens.

    Observação:
      Esta classe NÃO conhece CLI. Ela é uma peça de baixo nível.
    """

    def __init__(
        self,
        peer_id: str,
        host: str,
        porta: int,
        estado_local: EstadoPeerLocal,
        tabela: TabelaPeers,
        on_mensagem: Optional[Callable[[Dict[str, Any], "ConexaoPeer"], None]] = None,
    ) -> None:
        self.peer_id = peer_id
        self.host = host
        self.porta = int(porta)

        self.estado_local = estado_local
        self.tabela = tabela
        self.on_mensagem = on_mensagem

        self._sock: Optional[socket.socket] = None
        self._leitura_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Guarda a última resposta HELLO_OK
        self.hello_response: Optional[Dict[str, Any]] = None

        # Controle de RTT para PINGs na conexão persistente
        self._ping_sent: Dict[str, float] = {}
        self._lock = threading.Lock()
        
        
        
    def is_ativa(self) -> bool:
        """
        Indica se a conexão ainda está ativa:

        - Socket aberto (_sock != None)
        - Thread de leitura não foi sinalizada para parar (_stop_event não setado).
        """
        return self._sock is not None and not self._stop_event.is_set()


    # ------------------------------------------------------------------
    # Conexão / Handshake
    # ------------------------------------------------------------------

    def conectar(self, timeout_seg: float = 5.0) -> Optional[Dict[str, Any]]:
        """
        Abre o socket, envia HELLO e espera HELLO_OK.

        Retorna o dict da resposta HELLO_OK (ou None em erro).
        """
        if self._sock is not None:
            # Já conectado
            return self.hello_response

        self.tabela.registrar_ou_atualizar(self.peer_id, self.host, self.porta)
        self.tabela.marcar_conectando(self.peer_id)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_seg)

        try:
            sock.connect((self.host, self.porta))
        except OSError as e:
            print(f"[ConexaoPeer] Falha ao conectar em {self.host}:{self.porta}: {e}")
            self.tabela.registrar_falha(self.peer_id)
            self.tabela.marcar_desconectado(self.peer_id)
            return None

        # Conectou: podemos remover o timeout para leitura contínua
        sock.settimeout(None)
        self._sock = sock

        # Envia HELLO na conexão PERSISTENTE
        hello_pkt = {
            "type": "HELLO",
            "peer_id": f"{self.estado_local.nome}@{self.estado_local.namespace}",
            "version": "1.0",
            "features": ["ack", "metrics"],
            "ttl": 1,
        }

        if not self._enviar_linha_json(hello_pkt):
            print(f"[ConexaoPeer] Erro ao enviar HELLO para {self.host}:{self.porta}")
            self.fechar()
            return None

        # Lê uma única linha, esperando o HELLO_OK
        try:
            linha = self._ler_linha()
        except OSError as e:
            print(f"[ConexaoPeer] Erro ao ler HELLO_OK de {self.host}:{self.porta}: {e}")
            self.fechar()
            return None

        if linha is None:
            print(f"[ConexaoPeer] Conexão fechada antes do HELLO_OK por {self.host}:{self.porta}")
            self.fechar()
            return None

        try:
            pkt = json.loads(linha)
        except json.JSONDecodeError:
            print(f"[ConexaoPeer] Resposta inválida a HELLO: {linha!r}")
            self.fechar()
            return None

        if pkt.get("type") != "HELLO_OK":
            print(f"[ConexaoPeer] Resposta inesperada a HELLO: {pkt}")
            self.fechar()
            return pkt

        # Sucesso: registramos na tabela e iniciamos thread de leitura contínua
        self.hello_response = pkt
        self.tabela.marcar_ativo(self.peer_id)
        self._iniciar_loop_leitura()
        print(
            f"[ConexaoPeer] HELLO_OK recebido de {self.host}:{self.porta} "
            f"para peer_id={self.peer_id}."
        )
        return pkt

    # ------------------------------------------------------------------
    # Envio de mensagens na conexão persistente
    # ------------------------------------------------------------------

    def enviar_ping(self, msg_id: str) -> bool:
        """
        Envia PING via conexão persistente e registra o timestamp de envio
        para cálculo de RTT quando o PONG chegar.
        """
        if self._sock is None:
            print(f"[ConexaoPeer] Não há socket para enviar PING a {self.peer_id}.")
            return False

        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        pkt = {
            "type": "PING",
            "msg_id": msg_id,
            "timestamp": now_iso,
            "ttl": 1,
        }

        with self._lock:
            self._ping_sent[msg_id] = time.monotonic()

        ok = self._enviar_linha_json(pkt)
        if not ok:
            # falha de envio → limpa o registro do ping
            with self._lock:
                self._ping_sent.pop(msg_id, None)
        else:
            print(f"[ConexaoPeer] PING enviado para {self.peer_id} (msg_id={msg_id}).")
        return ok

    def enviar_send(self, payload: str, dst_peer_id: Optional[str] = None) -> bool:
        """
        Envia SEND via conexão persistente.
        (Aqui ainda não calculamos RTT por msg_id; podemos estender depois.)
        """
        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        msg_id = f"msg-{now_iso}"
        pkt = {
            "type": "SEND",
            "msg_id": msg_id,
            "timestamp": now_iso,
            "ttl": 1,
            "src": f"{self.estado_local.nome}@{self.estado_local.namespace}",
            "dst": dst_peer_id or self.peer_id,
            "require_ack": True,
            "payload": payload,
        }
        print(f"[ConexaoPeer] SEND para {self.peer_id}: {payload!r} (msg_id={msg_id})")
        return self._enviar_linha_json(pkt)
    
    def enviar_pub(self, payload: str, dst: str = "*") -> bool:
            """
            Envia PUB via conexão persistente.

            - dst: pode ser "*" ou algo como "#UnB" (namespace lógico).
            - payload: texto da mensagem.
            """
            if self._sock is None:
                print(f"[ConexaoPeer] Não há socket para enviar PUB a {self.peer_id}.")
                return False

            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            msg_id = f"pub-{now_iso}"
            pkt = {
                "type": "PUB",
                "msg_id": msg_id,
                "timestamp": now_iso,
                "ttl": 1,
                "src": f"{self.estado_local.nome}@{self.estado_local.namespace}",
                "dst": dst,
                "require_ack": False,
                "payload": payload,
            }

            print(f"[ConexaoPeer] PUB para {self.peer_id}: {payload!r} (msg_id={msg_id}, dst={dst})")
            return self._enviar_linha_json(pkt)


    def enviar_bye(self) -> bool:
        """
        Envia BYE pedindo encerramento gracioso **apenas desta conexão**.

        Não tenta ler BYE_OK aqui para não concorrer com o loop de leitura.
        A thread de leitura (_loop_leitura) continuará lendo até:
        - receber BYE_OK, ou
        - a conexão ser fechada pelo remoto.
        Em ambos os casos, _stop_event já estará setado e o loop vai encerrar.
        """
        if self._sock is None:
            print(f"[ConexaoPeer] Não há socket para enviar BYE a {self.peer_id}.")
            return False

        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        pkt = {
            "type": "BYE",
            "timestamp": now_iso,
            "ttl": 1,
        }
        print(f"[ConexaoPeer] Enviando BYE para {self.peer_id}.")

        ok = self._enviar_linha_json(pkt)
        if ok:
            # Sinaliza para o loop de leitura que queremos encerrar esta conexão
            self._stop_event.set()
        return ok

    # ------------------------------------------------------------------
    # Loop de leitura
    # ------------------------------------------------------------------

    def _iniciar_loop_leitura(self) -> None:
        """
        Inicia thread em modo daemon para ficar lendo mensagens
        enquanto a conexão existir.
        """
        thread = threading.Thread(target=self._loop_leitura, daemon=True)
        self._leitura_thread = thread
        thread.start()

    def _loop_leitura(self) -> None:
        """
        Loop contínuo de leitura de linhas JSON terminadas em '\n'.
        """
        while not self._stop_event.is_set():
            try:
                linha = self._ler_linha()
            except OSError as e:
                print(f"[ConexaoPeer] Erro de leitura de {self.host}:{self.porta}: {e}")
                break

            if linha is None:
                # Conexão fechada
                print(f"[ConexaoPeer] Conexão encerrada por {self.host}:{self.porta}.")
                break

            try:
                pkt = json.loads(linha)
            except json.JSONDecodeError:
                print(f"[ConexaoPeer] Mensagem JSON inválida de {self.host}:{self.porta}: {linha!r}")
                continue

            self._tratar_mensagem(pkt)

        # Saiu do loop → marcamos como desconectado e fechamos
        self.tabela.marcar_desconectado(self.peer_id)
        self.fechar()

    def _tratar_mensagem(self, pkt: Dict[str, Any]) -> None:
        """
        Lida com mensagens recebidas na conexão persistente.
        Atualiza a TabelaPeers e, quando possível, RTT.
        """
        tipo = pkt.get("type")

        # Qualquer mensagem indica atividade recente
        self.tabela.marcar_ativo(self.peer_id)

        if tipo == "PING":
            # Responder PONG na MESMA conexão
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            resp = {
                "type": "PONG",
                "msg_id": pkt.get("msg_id", "uuid"),
                "timestamp": now_iso,
                "ttl": 1,
            }
            self._enviar_linha_json(resp)
            print(f"[ConexaoPeer] PING recebido de {self.peer_id}, PONG enviado.")

        elif tipo == "PONG":
            msg_id = pkt.get("msg_id")
            rtt_ms: Optional[float] = None
            if msg_id:
                with self._lock:
                    t0 = self._ping_sent.pop(msg_id, None)
                if t0 is not None:
                    rtt_ms = (time.monotonic() - t0) * 1000.0
                    self.tabela.atualizar_rtt(self.peer_id, rtt_ms)
            print(f"[ConexaoPeer] PONG recebido de {self.peer_id} (msg_id={msg_id}), RTT={rtt_ms:.2f} ms"
                  if rtt_ms is not None else
                  f"[ConexaoPeer] PONG recebido de {self.peer_id} (msg_id={msg_id}).")

        elif tipo == "SEND":
            print(f"[ConexaoPeer] SEND recebido de {self.peer_id}: {pkt.get('payload')!r}")
            # FUTURO: responder ACK pela conexão persistente, se a especificação pedir.

        elif tipo == "ACK":
            print(f"[ConexaoPeer] ACK recebido de {self.peer_id}: {pkt}")
            
        elif tipo == "PUB":
            payload = pkt.get("payload", "")
            dest_raw = pkt.get("dst", "*")

            if dest_raw == "*":
                destino_fmt = "@todos"
            elif isinstance(dest_raw, str) and dest_raw.startswith("#"):
                destino_fmt = f"#{dest_raw}"
            else:
                destino_fmt = f"#{dest_raw}"

            print(f'[MSG][De: {self.peer_id}][{destino_fmt}] "{payload}"')



        elif tipo == "BYE":
            print(f"[ConexaoPeer] BYE recebido de {self.peer_id}, enviando BYE_OK e encerrando.")
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            resp = {
                "type": "BYE_OK",
                "timestamp": now_iso,
                "ttl": 1,
            }
            self._enviar_linha_json(resp)
            self._stop_event.set()

        elif tipo == "BYE_OK":
            print(f"[ConexaoPeer] BYE_OK recebido de {self.peer_id}. Encerrando conexão.")
            self._stop_event.set()


        else:
            print(f"[ConexaoPeer] Mensagem desconhecida de {self.peer_id}: {pkt}")

        # Callback opcional para integração com ClienteP2P/CLI
        if self.on_mensagem is not None:
            try:
                self.on_mensagem(pkt, self)
            except Exception as e:  # evitar queda da thread
                print(f"[ConexaoPeer] Erro no callback on_mensagem: {e}")

    # ------------------------------------------------------------------
    # Utilitários de envio/recebimento linha-a-linha
    # ------------------------------------------------------------------

    def _enviar_linha_json(self, data: Dict[str, Any]) -> bool:
        """
        Serializa dict como JSON + '\n' e envia pela conexão.
        """
        if self._sock is None:
            print(f"[ConexaoPeer] Tentativa de envio sem socket para {self.peer_id}.")
            return False

        linha = json.dumps(data, ensure_ascii=False) + "\n"
        try:
            self._sock.sendall(linha.encode("utf-8"))
            return True
        except OSError as e:
            print(f"[ConexaoPeer] Erro ao enviar para {self.peer_id}: {e}")
            self.tabela.registrar_falha(self.peer_id)
            self.tabela.marcar_stale(self.peer_id)
            return False

    def _ler_linha(self) -> Optional[str]:
        """
        Lê bytes até encontrar '\n' ou o socket ser fechado.

        Retorna:
          - string sem '\n' no final, se sucesso;
          - None se a conexão foi encerrada.
        """
        if self._sock is None:
            return None

        buffer = bytearray()
        while True:
            chunk = self._sock.recv(4096)
            if not chunk:
                # Conexão fechada
                return None
            buffer.extend(chunk)
            if b"\n" in chunk:
                break

        linha, _, _resto = buffer.partition(b"\n")
        return linha.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------
    # Fechamento
    # ------------------------------------------------------------------

    def fechar(self) -> None:
        """
        Encerra a conexão de forma imediata (sem enviar BYE).
        Para BYE gracioso, chame enviar_bye() antes.
        """
        self._stop_event.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            finally:
                self._sock = None

        self.tabela.marcar_desconectado(self.peer_id)
