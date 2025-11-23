import json
import socket
from pathlib import Path
#from .packetman import Packet
from packetman import Packet


class Connection:
    """Gerencia a conexão dos peers e armazena nome, namespace, ip, porta do peer atual."""
    def __init__(self):
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

        self.port = peer_info["listen_port"]
        self.ttl = 7200

        #rendezvous
        self.rendezvous = "pyp2p.mfcaetano.cc"
        self.rendezvousPort = 8080

    def register(self):
        """Registra no servidor utilizando o nome e namespace no arquivo peer_info.json. Movido da classe Packet."""
        #f = open("peer_info.json", "r")
        #peer_info = json.loads(f.read())
        #packet = Packet()
        try:
            base_dir = Path(__file__).resolve().parent
            peer_file = base_dir / "peer_info.json"

            with peer_file.open("r", encoding="utf-8") as f:
                peer_info = json.loads(f.read())

            packet = Packet()




            self.name = peer_info["nome"]
            self.namespace = peer_info["namespace"]
            self.port = int(peer_info["listen_port"])
            self.ttl = 7200
            type = "REGISTER"
            packet.create(type, namespace=self.namespace, name=self.name, port=self.port, ttl=self.ttl) # Cria o packet tipo REGISTER
            response = packet.send(self.rendezvous, self.rendezvousPort)
            if response["status"] == "OK":
                print("[REGISTER] Conexão feita com sucesso!")
            else:
                print(f"[REGISTER] Erro de rendezvous, packet recebido: \n{json.dumps(response)}")
        except Exception as e:
            print(f"Erro: \n{e}\nNOTA: O arquivo de peer_info está certo?")
    
    def discover(self, namespace: str | None = None):
        """Manda um discover ao server rendezvous. Necessário estar registrado."""
        packet = Packet()
        

        if namespace:
            # DISCOVER filtrando por namespace (ex.: "UnB", "CIC", etc.)
            packet.create("DISCOVER", namespace=namespace)

        else:
            # DISCOVER global
            packet.createDiscover()

        response = packet.send(self.rendezvous, self.rendezvousPort)

        if not response:
            print("[DISCOVER] Sem resposta do servidor rendezvous.")
            return

        if response.get("status") == "OK":
            print("[DISCOVER] Status OK!")
            self.latestDiscover = response.get("peers", [])

        else:
            print(f"[DISCOVER] Erro de rendezvous, packet recebido: \n{json.dumps(response, indent=2)}")

    def latestPeers(self, repeat : bool = False):
        """
        Printa os últimos peers descobertos des do ultimo discover.

        :param repeat: Se verdadeiro, executa um discover antes de printar.
        """
        if repeat:
            self.discover()
        print("[PEERS] Seguem os relevantes peers descobertos des do ultimo discover.")
        print(json.dumps(self.latestDiscover, indent=4))

    def hello(self, host, port):
        """
        Envia um HELLO para o endereço e o port.

        :param host: Endereço do remetente.
        :param port: Port do remetente.
        :return: Resposta do remetente.
        :rtype: Dict | None
        """
        packetType = "HELLO"
        packet = Packet()

        packet.create(type=packetType, 
                      peer_id=f"{self.name}@{self.namespace}", 
                      version="1.0", features=["ack", "metrics"], 
                      ttl=1)
        

        print(f"[HELLO] Enviando HELLO para {host}:{port}.")
        self.peerAddr = host
        self.peerPort = port


        for i in self.latestDiscover:
            if i.get("ip") == host and i.get("port") == port:
                self.peerName = i.get("name","")
                self.peerNamespace = i.get("namespace", "")
                print("[HELLO] Encontrado ip do peer na ultima lista discover. Nome e namespace de peer atualizados.")
        response = packet.send(self.peerAddr, self.peerPort)

        if not response:
            print("[HELLO] Sem resposta do peer.")
            return None


        if response.get("type") == "HELLO_OK":
            peer_id = response.get("peer_id", "")
            if not self.peerName and "@" in peer_id:
                name, ns = peer_id.split("@", 1)
                self.peerName = name
                self.peerNamespace = ns
            print(f"[HELLO] HELLO_OK recebido de {self.peerName}@{self.peerNamespace} (endereço {host}:{port}).")
            return response
        else:
            print(f"[HELLO] Resposta inesperada: {json.dumps(response, indent=2)}")
            return response
        