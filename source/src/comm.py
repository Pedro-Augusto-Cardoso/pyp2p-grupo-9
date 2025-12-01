import json
import socket
from pathlib import Path
#from .packetman import Packet
from packetman import Packet


class Connection:
    """
    Gerencia a conexão deste peer:
    - Lê nome, namespace e porta de peer_info.json
    - Faz REGISTER / DISCOVER no servidor Rendezvous
    - Mantém a última lista de peers descobertos
    - Implementa HELLO como cliente e como servidor (process)
    """

    def __init__(self) -> None:
        # Carrega configuração local do peer
        base_dir = Path(__file__).resolve().parent
        peer_file = base_dir / "peer_info.json"

        with peer_file.open("r", encoding="utf-8") as f:
            peer_info = json.load(f)

        self.peerAddr = ""
        self.peerName = ""
        self.peerNamespace = ""
        self.peerPort = 0
        self.latestDiscover = []

        #info local
        self.name = peer_info["nome"]
        self.namespace = peer_info["namespace"]

        # Infra de servidor TCP local
        self.tcpServer: Optional[socket.socket] = None
        self.tcpClient: Optional[threading.Thread] = None

        # Configuração do Rendezvous
        self.rendezvous: str = "pyp2p.mfcaetano.cc"
        self.rendezvousPort: int = 8080
        self.ttl: int = 7200  # segundos

        # Tabelas auxiliares para extensões futuras (RTT, conn, etc.)
        self.rtt: Dict[str, float] = {}
        self.connections: Dict[str, Dict[str, Any]] = {}

    # ----------------------------------------------------------------------
    # === Integração com o Rendezvous =====================================
    # ----------------------------------------------------------------------

    def register(self) -> None:
        """
        Registra este peer no servidor Rendezvous usando os dados de peer_info.json.
        """
        packet = Packet().create(
            "REGISTER",
            namespace=self.namespace,
            name=self.name,
            port=self.port,
            ttl=self.ttl,
        )

        try:
            response = packet.send(self.rendezvous, self.rendezvousPort)
        except OSError as e:
            print(f"[REGISTER] Erro ao conectar no rendezvous: {e}")
            return

        if not response:
            print("[REGISTER] Sem resposta do servidor rendezvous.")
            return

        if response.get("status") == "OK":
            print("[REGISTER] Registro realizado com sucesso!")
        else:
            print(
                "[REGISTER] Erro de rendezvous, resposta recebida:\n"
                f"{json.dumps(response, indent=2)}"
            )

    def discover(self, namespace: Optional[str] = None) -> None:
        """
        Envia um DISCOVER ao servidor Rendezvous e atualiza self.latestDiscover.

        :param namespace: Namespace a ser filtrado (ex.: "UnB", "CIC").
                          Se None, faz DISCOVER global.
        """
        packet = Packet()
        if namespace:
            packet.create("DISCOVER", namespace=namespace)
        else:
            # DISCOVER global (sem namespace)
            packet.create("DISCOVER")

        try:
            response = packet.send(self.rendezvous, self.rendezvousPort)
        except OSError as e:
            print(f"[DISCOVER] Erro ao conectar no rendezvous: {e}")
            return

        if not response:
            print("[DISCOVER] Sem resposta do servidor rendezvous.")
            return

        if response.get("status") == "OK":
            self.latestDiscover = response.get("peers", [])
            print(f"[DISCOVER] OK. {len(self.latestDiscover)} peers recebidos.")
        else:
            print(
                "[DISCOVER] Erro de rendezvous, resposta recebida:\n"
                f"{json.dumps(response, indent=2)}"
            )

    def latestPeers(self, repeat: bool = False) -> None:
        """
        Mostra os últimos peers descobertos.

        :param repeat: Se True, executa um DISCOVER antes de mostrar.
        """
        if repeat:
            self.discover(self.namespace)

        print("[PEERS] Últimos peers descobertos:")
        if not self.latestDiscover:
            print("  (nenhum peer encontrado ainda)")
            return

        print(json.dumps(self.latestDiscover, indent=4))

    # ----------------------------------------------------------------------
    # === Comunicação direta entre peers ==================================
    # ----------------------------------------------------------------------

    def hello(self, host: str, port: int) -> Optional[Dict[str, Any]]:
        """
        Envia um HELLO para um peer específico (host, port).

        :param host: IP ou hostname do peer.
        :param port: Porta TCP do peer.
        :return: Resposta do peer (dict) ou None em caso de falha.
        """
        packet = Packet().create(
            "HELLO",
            peer_id=f"{self.name}@{self.namespace}",
            version="1.0",
            features=["ack", "metrics"],
            ttl=1,
        )

        print(f"[HELLO] Enviando HELLO para {host}:{port}.")
        self.peerAddr = host
        self.peerPort = port

        # Tenta encontrar host/port na última lista de DISCOVER para
        # preencher peerName / peerNamespace (apenas para log).
        for p in self.latestDiscover:
            if p.get("ip") == host and int(p.get("port", 0)) == int(port):
                self.peerName = p.get("name", "")
                self.peerNamespace = p.get("namespace", "")
                print("[HELLO] Peer localizado na última lista DISCOVER; nome/namespace atualizados.")
                break

        try:
            response = packet.send(host, port)
        except OSError as e:
            print(f"[HELLO] Erro ao conectar com o peer {host}:{port}: {e}")
            return None

        if not response:
            print("[HELLO] Sem resposta do peer.")
            return None

        if response.get("type") == "HELLO_OK":
            peer_id = response.get("peer_id", "")
            if "@" in peer_id:
                name, ns = peer_id.split("@", maxsplit=1)
                self.peerName = name
                self.peerNamespace = ns
            print(f"[HELLO] HELLO_OK recebido de {self.peerName}@{self.peerNamespace} ({host}:{port}).")
            return response
        else:
            print(f"[HELLO] Resposta inesperada: {json.dumps(response, indent=2)}")
            return response
        
