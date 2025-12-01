# cliente_p2p.py
from __future__ import annotations

import datetime
import json
import socket
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from estado import EstadoPeerLocal, ConfigRendezvous
from conexao_rendezvous import ConexaoRendezvous
from packetman import Packet
from tabela_peers import TabelaPeers
from conexao_peer import ConexaoPeer
import time



class ClienteP2P:
    """
    Versão "abrasileirada" da antiga classe Connection.

    Responsabilidades:
      - Carregar identidade local (peer_info.json).
      - Integrar com o Rendezvous via ConexaoRendezvous.
      - Manter servidor TCP para receber PING/SEND/HELLO.
      - Expor os atributos/métodos esperados pela CLI:
          .name, .namespace, .port
          .latestDiscover, .rtt
          register(), discover(), latestPeers(), hello(), startListen()
    """

    def __init__(self, peer_info_path: Optional[Path] = None) -> None:
        # --------------------------------------------------------------
        # Carrega configuração local do peer (peer_info.json)
        # --------------------------------------------------------------
        base_dir = Path(__file__).resolve().parent

        if peer_info_path is None:
            peer_file = base_dir / "peer_info.json"
        else:
            peer_file = Path(peer_info_path)

        with peer_file.open("r", encoding="utf-8") as f:
            peer_info = json.load(f)

        estado_peer = EstadoPeerLocal(
            nome=peer_info["nome"],
            namespace=peer_info["namespace"],
            porta_escuta=int(peer_info["listen_port"]),
        )

        self.estado_peer: EstadoPeerLocal = estado_peer
        self.cfg_rendezvous: ConfigRendezvous = ConfigRendezvous()
        


        # Para manter compatibilidade com a CLI atual:
        self.name: str = estado_peer.nome
        self.namespace: str = estado_peer.namespace
        self.port: int = estado_peer.porta_escuta
        
        # Identificador completo do peer local (compatível com código legado)
        self.peer_id_local: str = f"{self.name}@{self.namespace}"

        # Info básica de peers
        self.latestDiscover: List[Dict[str, Any]] = []

        # Infra de servidor TCP local
        self._tcp_server: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None

        # Métricas de RTT calculadas pela CLI (/ping, /msg)
        self.rtt: Dict[str, float] = {}

        # Dados auxiliares para HELLO (apenas para logs)
        self.peerAddr: str = ""
        self.peerName: str = ""
        self.peerNamespace: str = ""
        self.peerPort: int = 0

        # Conexão com o servidor Rendezvous
        self._rendezvous = ConexaoRendezvous(self.estado_peer, self.cfg_rendezvous)
        
        self.tabela_peers = TabelaPeers()
        self.conexoes: Dict[str, ConexaoPeer] = {}
        
        # Monitor de keep-alive (PING automático)
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitor_stop = threading.Event()
        #self._iniciar_monitoramento_conexoes()
        
        # Automação de rendezvous (REGISTER/DISCOVER + auto-HELLO)
        self._auto_rdv_thread: Optional[threading.Thread] = None
        self._auto_rdv_stop = threading.Event()
        
        # 1) Sobe o servidor TCP logo na inicialização, para já aceitar HELLO/PING/SEND
        self.startListen()
        
        # 2) Inicia monitor de keep-alive (PING periódico em conexões persistentes)
        self._iniciar_monitoramento_conexoes()
        
        # 3) Inicia automação com o Rendezvous:
        #    - REGISTER periódico
        #    - DISCOVER periódico
        #    - Tentativa de HELLO automático com todos os peers descobertos
        self._iniciar_automacao_rendezvous()
        
        
    # ------------------------------------------------------------------
    # Auto-conexão com peers descobertos
    # ------------------------------------------------------------------

    def _conectar_peers_descobertos(self) -> None:
        """
        Tenta estabelecer conexões PERSISTENTES (HELLO) com todos os peers
        conhecidos na TabelaPeers, exceto o próprio peer local.

        - Só tenta peers que NÃO têm conexão ativa (is_ativa() == False).
        """
        peers_snapshot = self.tabela_peers.listar()
        if not peers_snapshot:
            return

        for entry in peers_snapshot:
            peer_id = entry.peer_id

            # Nunca tenta conectar em si mesmo
            if peer_id == self.peer_id_local:
                continue

            # Verifica se já há conexão ativa
            conexao_existente = self.conexoes.get(peer_id)
            if conexao_existente is not None and conexao_existente.is_ativa():
                # Já está com sessão aberta e saudável
                continue

            host = entry.host
            porta = entry.porta
            if not host or not porta:
                continue

            print(f"[AUTO-CONN] Tentando HELLO automático com {peer_id} em {host}:{porta}...")

            # Se havia conexão antiga morta, limpa antes
            self.conexoes.pop(peer_id, None)

            conexao = ConexaoPeer(
                peer_id=peer_id,
                host=host,
                porta=porta,
                estado_local=self.estado_peer,
                tabela=self.tabela_peers,
                on_mensagem=None,  # callback opcional no futuro
            )

            resp = conexao.conectar()
            if resp is None:
                print(f"[AUTO-CONN] Falha ao conectar com {peer_id}. Marcando como STALE.")
                self.tabela_peers.marcar_stale(peer_id)
                # não guarda conexão morta no dicionário
            else:
                print(f"[AUTO-CONN] Conexão persistente estabelecida com {peer_id}.")
                self.conexoes[peer_id] = conexao



    # ------------------------------------------------------------------
    # Integração com Rendezvous (encapsula ConexaoRendezvous)
    # ------------------------------------------------------------------

    def register(self) -> None:
        """
        Wrapper para /register da CLI.
        """
        self._rendezvous.registrar()

    def discover(self, namespace: Optional[str] = None) -> None:
        """
        Wrapper para /discover da CLI.

        Atualiza self.latestDiscover com a última lista retornada
        e sincroniza a TabelaPeers com esses peers.
        """
        peers = self._rendezvous.discover(namespace)
        self.latestDiscover = peers

        # Alimenta TabelaPeers com os peers descobertos
        for p in peers:
            name = p.get("name") or "?"
            ns = p.get("namespace") or "?"
            ip = p.get("ip") or "127.0.0.1"
            port = p.get("listen_port") or p.get("port") or 0
            peer_id = p.get("peer_id") or f"{name}@{ns}"
            try:
                porta_int = int(port)
            except (TypeError, ValueError):
                porta_int = 0
            self.tabela_peers.registrar_ou_atualizar(peer_id, ip, porta_int)
            
            


    # ------------------------------------------------------------------
    # Automação de REGISTER / DISCOVER com Rendezvous
    # ------------------------------------------------------------------

    def _iniciar_automacao_rendezvous(self, intervalo_segundos: int = 30) -> None:
        """
        Inicia um thread em background que, periodicamente:

          - Faz REGISTER no Rendezvous (renova TTL).
          - Faz DISCOVER GLOBAL (todos os namespaces).
          - Atualiza TabelaPeers.
          - Tenta HELLO com todos os peers sem conexão ativa.

        Tudo isso acontece em paralelo à CLI.
        """
        if self._auto_rdv_thread is not None:
            return  # já iniciado

        def _loop() -> None:
            while not self._auto_rdv_stop.is_set():
                try:
                    print("[AUTO-RDV] Ciclo automático: REGISTER + DISCOVER (global) + auto-HELLO...")

                    # 1) Registrar no rendezvous (renova TTL)
                    self.register()

                    # 2) Discover GLOBAL: namespace = None
                    peers = self._rendezvous.discover(namespace=None)
                    self.latestDiscover = peers

                    # 3) Sincroniza TabelaPeers com peers descobertos
                    for p in peers:
                        name = p.get("name") or "?"
                        ns = p.get("namespace") or "?"
                        ip = p.get("ip") or "127.0.0.1"
                        port = p.get("listen_port") or p.get("port") or 0
                        peer_id = p.get("peer_id") or f"{name}@{ns}"
                        try:
                            porta_int = int(port)
                        except (TypeError, ValueError):
                            porta_int = 0
                        self.tabela_peers.registrar_ou_atualizar(peer_id, ip, porta_int)

                    # 4) Tenta HELLO com todos os peers sem conexão ativa
                    self._conectar_peers_descobertos()

                except Exception as e:
                    # Não deixa o thread morrer por exceção
                    print(f"[AUTO-RDV] Erro no ciclo automático: {e}")

                # Espera até o próximo ciclo ou até receber sinal de parada
                if self._auto_rdv_stop.wait(intervalo_segundos):
                    break

        self._auto_rdv_thread = threading.Thread(target=_loop, daemon=True)
        self._auto_rdv_thread.start()

    def parar_automacao_rendezvous(self) -> None:
        """
        Interrompe o loop automático de REGISTER/DISCOVER com o Rendezvous.
        """
        self._auto_rdv_stop.set()




    def unregister(self) -> None:
        """
        Wrapper para UNREGISTER no Rendezvous.

        Usado principalmente no /quit para sair "limpo".
        """
        try:
            self._rendezvous.remover_registro()
        except Exception as e:
            print(f"[UNREGISTER] Erro ao tentar remover registro: {e}")



    def latestPeers(self, repeat: bool = False) -> None:
        """
        Wrapper para /peers da CLI.

        Se repeat=True, faz DISCOVER no namespace local antes de exibir.
        """
        if repeat:
            self.discover(self.namespace)

        print("[PEERS] Últimos peers descobertos:")
        if not self.latestDiscover:
            print("  (nenhum peer encontrado ainda)")
            return

        # Mostra a lista no mesmo formato anterior, mas mais amigável
        for idx, p in enumerate(self.latestDiscover, start=1):
            name = p.get("name") or "?"
            ns = p.get("namespace") or "?"
            ip = p.get("ip") or "?"
            port = p.get("listen_port") or p.get("port") or "?"
            peer_id = p.get("peer_id") or f"{name}@{ns}"
            print(f"  {idx:3d}. {peer_id} ({ip}:{port}) ns={ns}")

    # ------------------------------------------------------------------
    # Servidor TCP local (recebe PING / SEND / HELLO)
    # ------------------------------------------------------------------

    def startListen(self, host: str = "0.0.0.0", port: Optional[int] = None) -> None:
        """
        Inicia o servidor TCP deste peer para receber conexões de outros peers.
        """
        if port is None:
            port = self.port

        if self._tcp_server is not None:
            print("[LISTEN] Servidor TCP já está em execução.")
            return

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()

        self._tcp_server = server
        print(f"[LISTEN] Servidor TCP ouvindo em {host}:{port}.")

        def _accept_loop() -> None:
            while True:
                try:
                    client, addr = server.accept()
                except OSError:
                    # Socket fechado externamente
                    break

                client_handler = threading.Thread(
                    target=self.process,
                    args=(client, addr),
                    daemon=True,
                )
                client_handler.start()
                print(f"[LISTEN] Conexão aceita de {addr[0]}:{addr[1]}.")

        thread = threading.Thread(target=_accept_loop, daemon=True)
        self._accept_thread = thread
        thread.start()

    def process(self, client: socket.socket, addr: Tuple[str, int]) -> None:
        """
        Processa uma conexão recebida no servidor TCP.
        Executado em uma thread separada.

        Nesta versão, a conexão é tratada como PERSISTENTE:
        - Mantemos o socket aberto e processamos múltiplas mensagens
        (HELLO, PING, SEND, PUB, BYE) na mesma conexão.
        - Fechamos apenas quando o peer fecha o socket ou envia BYE.
        """
        buffer = b""

        try:
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    # Peer fechou a conexão
                    print(f"[LISTEN] Conexão encerrada por {addr[0]}:{addr[1]}.")
                    break

                buffer += chunk

                # Processa todas as linhas completas que chegaram
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not line:
                        continue

                    print(f"[LISTEN] Recebido request de {addr[0]}:{addr[1]}: {line!r}")

                    packet = Packet(line)
                    packet.setType()

                    now = datetime.datetime.utcnow()
                    formatted_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

                    if packet.type == "PING":
                        # Responde com PONG na MESMA conexão
                        response = Packet().create(
                            "PONG",
                            msg_id=packet.content.get("msg_id", "uuid"),
                            timestamp=formatted_timestamp,
                            ttl=1,
                        )
                        try:
                            line_resp = response._to_line()
                            client.sendall(line_resp)
                            print(f"[LISTEN] PING recebido, PONG enviado para {addr[0]}:{addr[1]}.")
                        except OSError as e:
                            print(f"[LISTEN] Erro ao enviar PONG para {addr}: {e}")
                            return  # encerra o process() para este cliente

                    elif packet.type == "SEND":
                        # Confirma recebimento com ACK
                        payload = packet.content.get("payload")
                        src_peer = packet.content.get("src", f"{addr[0]}:{addr[1]}")

                        response = Packet().create(
                            "ACK",
                            msg_id=packet.content.get("msg_id", "uuid"),
                            timestamp=formatted_timestamp,
                            ttl=1,
                        )
                        try:
                            line_resp = response._to_line()
                            client.sendall(line_resp)
                            print(f"[LISTEN] SEND recebido de {src_peer}, ACK enviado para {addr[0]}:{addr[1]}.")
                            print("[MSG]", payload)
                        except OSError as e:
                            print(f"[LISTEN] Erro ao enviar ACK para {addr}: {e}")
                            return

                    elif packet.type == "PUB":
                        # Tratamento de broadcast recebido
                        payload = packet.content.get("payload", "")
                        origem = packet.content.get("src", f"{addr[0]}:{addr[1]}")
                        dest_raw = packet.content.get("dst", "*")

                        # Formata destino de forma amigável
                        if dest_raw == "*":
                            destino_fmt = "@todos"
                        elif isinstance(dest_raw, str) and dest_raw.startswith("#"):
                            destino_fmt = f"#{dest_raw}"
                        else:
                            destino_fmt = f"@{dest_raw}"

                        # Linha de mensagem legível
                        print(f'[MSG][De: {origem}][{destino_fmt}] "{payload}"')


                    elif packet.type == "HELLO":
                        # Handshake de boas-vindas — NÃO fechamos a conexão depois
                        response = Packet().create(
                            "HELLO_OK",
                            peer_id=f"{self.name}@{self.namespace}",
                            version="1.0",
                            features=["ack", "metrics"],
                            ttl=1,
                        )
                        try:
                            line_resp = response._to_line()
                            client.sendall(line_resp)
                            print(f"[LISTEN] HELLO recebido, HELLO_OK enviado para {addr[0]}:{addr[1]}.")
                        except OSError as e:
                            print(f"[LISTEN] Erro ao enviar HELLO_OK para {addr}: {e}")
                            return

                    elif packet.type == "BYE":
                        # Encerramento da sessão com BYE_OK, conforme especificação.
                        print(f"[LISTEN] BYE recebido de {addr[0]}:{addr[1]}. Enviando BYE_OK e encerrando conexão.")

                        response = Packet().create(
                            "BYE_OK",
                            timestamp=formatted_timestamp,
                            ttl=1,
                        )

                        try:
                            line_resp = response._to_line()
                            client.sendall(line_resp)
                            print(f"[LISTEN] BYE_OK enviado para {addr[0]}:{addr[1]}.")
                        except OSError as e:
                            print(f"[LISTEN] Erro ao enviar BYE_OK para {addr}: {e}")

                        # Depois de BYE/BYE_OK, encerramos a conexão
                        return

                    else:
                        print(f"[LISTEN] Tipo de mensagem não tratado: {packet.type}")
                        # Aqui poderíamos responder um ERRO, se a especificação exigir.

        finally:
            try:
                client.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # HELLO (cliente) – usado pela CLI /hello
    # ------------------------------------------------------------------

    def hello(self, host: str, port: int) -> Optional[Dict[str, Any]]:
        """
        Envia um HELLO para um peer específico (host, port) usando
        ConexaoPeer e estabelece uma conexão PERSISTENTE.

        Retorna a resposta HELLO_OK (ou None em caso de falha),
        mantendo a assinatura esperada pela CLI.
        """
        # Tenta descobrir peer_id a partir da última lista DISCOVER
        peer_id = None
        for p in self.latestDiscover:
            ip = p.get("ip")
            p_port = p.get("listen_port") or p.get("port")
            if ip == host and int(p_port or 0) == int(port):
                name = p.get("name") or "?"
                ns = p.get("namespace") or "?"
                peer_id = p.get("peer_id") or f"{name}@{ns}"
                self.peerName = name
                self.peerNamespace = ns
                break

        if peer_id is None:
            # Fallback: cria peer_id sintético
            peer_id = f"{host}:{port}"
            self.peerName = host
            self.peerNamespace = "?"

        self.peerAddr = host
        self.peerPort = port

        # Já existe conexão para esse peer?
        conexao = self.conexoes.get(peer_id)
        if conexao is None:
            conexao = ConexaoPeer(
                peer_id=peer_id,
                host=host,
                porta=port,
                estado_local=self.estado_peer,
                tabela=self.tabela_peers,
                on_mensagem=None,  # podemos ligar um callback no futuro
            )
            self.conexoes[peer_id] = conexao

        print(f"[HELLO] Estabelecendo conexão persistente com {peer_id} em {host}:{port}...")
        resp = conexao.conectar()
        if resp is None:
            print("[HELLO] Falha ao estabelecer conexão persistente.")
        else:
            print("[HELLO] Conexão persistente estabelecida com sucesso.")
        return resp

    
    # ------------------------------------------------------------------
    # Monitoramento de conexões (keep-alive automático)
    # ------------------------------------------------------------------

    def _iniciar_monitoramento_conexoes(self, intervalo_segundos: int = 30) -> None:
        """
        Inicia um thread em background que, periodicamente, envia
        PING para todos os peers com conexão persistente.

        Isso NÃO bloqueia a CLI e usa as métricas de RTT do ConexaoPeer.
        """
        if self._monitor_thread is not None:
            return  # já iniciado

        def _loop() -> None:
            while not self._monitor_stop.wait(intervalo_segundos):
                if not self.conexoes:
                    continue
                print("[MONITOR] Enviando PING de keep-alive para conexões ativas...")
                for peer_id, conexao in list(self.conexoes.items()):
                    msg_id = f"ka-{time.monotonic_ns()}"
                    conexao.enviar_ping(msg_id)

        self._monitor_thread = threading.Thread(target=_loop, daemon=True)
        self._monitor_thread.start()

    def parar_monitoramento_conexoes(self) -> None:
        """
        Permite interromper o monitor de keep-alive (por exemplo, em /quit).
        """
        self._monitor_stop.set()
        
        
    # ------------------------------------------------------------------
    # Encerramento limpo do cliente P2P
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """
        Encerra o cliente P2P de forma ordenada:

          - Para monitor de keep-alive.
          - Para automação de Rendezvous.
          - Envia BYE para conexões persistentes.
          - Fecha sockets de conexões persistentes.
          - Fecha o servidor TCP de escuta.
        """
        print("[SHUTDOWN] Iniciando encerramento do cliente P2P...")

        # 1) Para monitor de keep-alive
        self.parar_monitoramento_conexoes()
        
        # 1b) Para automação de Rendezvous
        self.parar_automacao_rendezvous()

        # 2) Envia BYE para cada conexão persistente (sem bloquear infinito)
        for peer_id, conexao in list(self.conexoes.items()):
            try:
                conexao.enviar_bye()
            except Exception as e:
                print(f"[SHUTDOWN] Erro ao enviar BYE para {peer_id}: {e}")
            # Não vamos esperar BYE_OK agora (poderia ser estendido)
            try:
                conexao.fechar()
            except Exception as e:
                print(f"[SHUTDOWN] Erro ao fechar conexão com {peer_id}: {e}")

        self.conexoes.clear()

        # 3) Fecha servidor TCP local
        if self._tcp_server is not None:
            print("[SHUTDOWN] Fechando servidor TCP local...")
            try:
                self._tcp_server.close()
            except Exception:
                pass
            finally:
                self._tcp_server = None

        print("[SHUTDOWN] Encerramento concluído.")




