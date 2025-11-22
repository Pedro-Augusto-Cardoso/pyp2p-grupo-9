import json
import socket
from .packetman import Packet

class Connection:
    """Gerencia a conexão dos peers e armazena nome, namespace, ip, porta do peer atual."""
    def __init__(self):
        f = open("peer_info.json", "r")
        peer_info = json.loads(f.read())
        self.peerAddr = ""
        self.peerName = ""
        self.peerNamespace = ""
        self.peerPort = 0
        self.latestDiscover = []
        self.name = peer_info["nome"]
        self.namespace = peer_info["namespace"]
        self.port = peer_info["listen_port"]
        self.ttl = 0
        self.rendezvous = "pyp2p.mfcaetano.cc"
        self.rendezvousPort = 8080

    def register(self):
        """Registra no servidor utilizando o nome e namespace no arquivo peer_info.json. Movido da classe Packet."""
        f = open("peer_info.json", "r")
        peer_info = json.loads(f.read())
        packet = Packet()
        try:
            self.name = peer_info["nome"]
            self.namespace = peer_info["namespace"]
            self.port = peer_info["listen_port"]
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
    
    def discover(self):
        """Manda um discover ao server rendezvous. Necessário estar registrado."""
        packet = Packet.createDiscover()
        response = packet.send(self.rendezvous, self.rendezvousPort)
        if response["status"] == "OK":
            print("[DISCOVER] Status OK!")
            self.latestDiscover = response.relevant("peers")

    def latestPeers(self, repeat : bool = False):
        """
        Printa os últimos peers descobertos des do ultimo discover.

        :param repeat: Se verdadeiro, executa um discover antes de printar.
        """
        if repeat:
            self.discover()
        print("[PEERS] Seguem os relevantes peers descobertos des do ultimo discover.")
        print(json.dumps(self.latestDiscover))

    def hello(self, host, port):
        """
        Envia um HELLO para o endereço e o port.

        :param host: Endereço do remetente.
        :param port: Port do remetente.
        :return: Resposta do remetente.
        :rtype: Dict | None
        """
        packetType = "HELLO"
        packet = Packet.create(type=packetType, peer_id=f"{self.name}@{self.namespace}", version="1.0", features=["ack", "metrics"], ttl=1)
        print(f"[HELLO] Enviando HELLO para {host}:{port}.")
        self.peerAddr = host
        self.peerPort = port
        for i in self.latestDiscover:
            if i["ip"] == host:
                self.peerName = i["name"]
                self.peerNamespace = i["namespace"]
                print("[HELLO] Encontrado ip do peer na ultima lista discover. Nome e namespace de peer atualizados.")
        response = packet.send(self.peerAddr, self.peerPort)
        if response["type"] == "HELLO_OK":
            print(f"[HELLO] HELLO_OK Recebido de {self.peerName}@{self.peerNamespace} (endereço {host}:{port}).\nDica: Se não há nada entre o \"@\", considere usar discover.")
            return response
        