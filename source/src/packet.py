import json
import socket

class Packet:
    def __init__(self):
        self.type = "none"
        self.content = json.dumps("{}", indent=4)

    def create(self, type, **kwargs):
        """
        Faz um packet genérico com o tipo e os argumentos. Cada argumento tem um nome e um conteudo, sendo esses colocados em json e retornados.

        :param type: Tipo do packet.
        :param **kwargs: Chaves do packet.
        """
        packet = {
            "type": type
        }
        for key, content in kwargs.items():
            packet[key] = content
        print("PACKET CRIADO:", str(packet))
        json_packet = json.dumps(packet, indent=4)
        self.content = json_packet

    def createRegister(self):
        """
        Cria mensagem de register baseada no arquivo peer_info.json
        """
        f = open("peer_info.json", "r")
        peer_info = json.loads(f.read())
        try:
            name = peer_info["nome"]
            namespace = peer_info["namespace"]
            port = peer_info["listen_port"]
            ttl = 7200
            type = "REGISTER"
            self.create(type, namespace=namespace, name=name, port=port, ttl=ttl) # Cria o packet tipo DISCOVER
        except Exception as e:
            print(f"Erro {e}.\nO arquivo de peer_info está certo?")

    def send(self, host, port):
        """
        Manda o packet para o host especificado.
        :param host: Endereço do host. Pode ser um IP ou um domínio.
        :param port: Port do host.
        :return: Packet com tipo Response e chave body.
        :rtype: Packet
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.sendall(bytes(self.content, encoding="utf-8"))


            received = sock.recv(1024)
            received = received.decode("utf-8")
            print("[DEBUG] Enviado packet TCP com conteúdo", self.content, f"para {host}:{port}.")
            print(f"[DEBUG] Recebido de {host}:{port}", received, "retornando como Packet.")
            return Packet.create("Response", body=received)
        except Exception as e:
            print("[DEBUG] Algo deu errado. Erro:", e)
            print(f"[DEBUG] Content enviado: {self.content}")

    def relevant(self):
        """
        Printa informações relevantes ao packet.
        """
        # Retorna conteúdo relevante do packet.
        content = self.content
        contentDict = json.loads(content)
        for key, value in contentDict.items():
            if key == "type":
                print(f"[RELEVANT] Packet de tipo {value}.")
            elif key == "body":
                print(f"[RELEVANT] Packet tem BODY, provávelmente é resposta. Conteudo: {value}")
            elif key == "name":
                print(f"[RELEVANT] Packet tem NAME, provável register. Nome: {value}")
            
