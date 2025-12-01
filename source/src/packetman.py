import json
import logging
import socket
from typing import Any, Dict, Optional
import asyncio

# Tamanho máximo de um pacote (em bytes) para evitar abusos / erros
MAX_PACKET_SIZE = 32 * 1024  # 32 KiB

logger = logging.getLogger(__name__)

class Packet:
    """Cria, envia e armazena packets. Contém métodos para criar packets específicos ou arbitrários."""
    def __init__(self):
        self.type = "none"
        self.content = json.dumps("{}", indent=4)

    def __init__(self, packet: Optional[Any] = None, type: str = "none") -> None:
        # Tipo genérico do pacote (HELLO, PING, SEND, ACK, etc.)
        self.type: str = type
        # Conteúdo em forma de dict (JSON)
        self.content: Dict[str, Any] = {}

        if packet is None:
            return

        # Se já veio como dict/list, usa direto
        if isinstance(packet, (dict, list)):
            self.content = packet  # type: ignore[assignment]

        # Se veio como bytes/str, tenta interpretar como JSON
        elif isinstance(packet, (bytes, str)):
            if isinstance(packet, bytes):
                packet = packet.decode("utf-8").strip()
            if packet:
                try:
                    decoded = json.loads(packet)
                    if isinstance(decoded, dict):
                        self.content = decoded
                    else:
                        # Aceita também lista ou outro tipo JSON, mas o mais comum é dict
                        self.content = {"payload": decoded}
                except json.JSONDecodeError:
                    logger.error("Falha ao decodificar JSON do pacote recebido: %r", packet)
                    self.content = {}
        else:
            raise TypeError(f"Tipo não suportado para Packet: {type(packet)}")

        # Atualiza type se estiver presente no conteúdo
        if isinstance(self.content, dict) and "type" in self.content:
            self.type = self.content["type"]

    # ------------------------------------------------------------------ #
    #      Métodos de manipulação de conteúdo / tipo do pacote          #
    # ------------------------------------------------------------------ #

    def setType(self) -> None:
        """
        Atualiza self.type a partir de self.content["type"], se existir.
        """
        if isinstance(self.content, dict):
            self.type = self.content.get("type", "none")
        else:
            self.type = "none"

    def create(self, type: str, **kwargs: Any) -> "Packet":
        """
        Cria um pacote simples com `type` e campos extras passados em kwargs.

        Exemplo:
            Packet().create("PING", msg_id="123", ttl=1)

        :param type: Tipo do packet.
        :param **kwargs: Chaves do packet.
        """
        self.content = {"type": type, **kwargs}
        self.type = type
        return self

    # def createRegister(self):
    #     """
    #     Cria mensagem de register baseada no arquivo peer_info.json
    #     """
        

    def createDiscover(self):
        """Wrapper para criar um packet tipo Discover.
        
        :return: Packet criado.
        :rtype: Packet
        """
        return self.create("DISCOVER")

    def send(self, host : str, port : int, maintain : bool = False):
        """
        Manda o packet para o host especificado.

        :param host: Endereço do host. Pode ser um IP ou um domínio.
        :param port: Port do host.
        :param maintain: Se a conexão deve ser mantida.
        :return: Json da resposta.
        :rtype: Dict
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.connect((host, port))
            logger.debug("Conectado a %s:%s", host, port)

            line = self._to_line()
            sock.sendall(line)

            # Lê resposta até encontrar '\\n' ou EOF
            chunks: list[bytes] = []
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                if b"\n" in chunk:
                    break

            received_str = received.decode("utf-8").strip()

            raw = b"".join(chunks)
            # Considera apenas até o primeiro '\\n'
            raw_line = raw.split(b"\n", 1)[0].decode("utf-8").strip()

            if not raw_line:
                logger.warning("Resposta vazia (linha em branco) de %s:%s", host, port)
                return {}, ack

            print("[DEBUG] Enviado packet TCP com conteúdo", self.content, f"para {host}:{port}.")
            print(f"[DEBUG] Recebido de {host}:{port}", received, "retornando como Packet.")
            if not received_str:
                return {}
            return json.loads(received_str)

        except Exception as e:
            print("[DEBUG] Algo deu errado. Erro:", e)
            print(f"[DEBUG] Content enviado: {self.content}")
            return {}
        finally:
            if not maintain:
                sock.close()

    def relevant(self, target : str = ""):
        """
        Printa informações relevantes ao packet.

        :param target: Qual key a ser explorada. Se vázio, retorna handling básico do packet.
        :return: Retorna o valor do target, se nãO ouver target não há retorno.
        :rtype: Any | None
        """
        # Retorna conteúdo relevante do packet.
        content = self.content
        contentDict = json.loads(content)
        if target == "": # Handling básico.
            for key, value in contentDict.items():
                if key == "type":
                    print(f"[RELEVANT] Packet de tipo {value}.")
                elif key == "body":
                    print(f"[RELEVANT] Packet tem BODY, provávelmente é resposta. Conteudo: {value}")
                elif key == "name":
                    print(f"[RELEVANT] Packet tem NAME, provável register. Nome: {value}")
                
        else:
            print(f"[RELEVANT] Conteúdo de chave {target} é {str(contentDict[target])}")
            return contentDict[target]
            
