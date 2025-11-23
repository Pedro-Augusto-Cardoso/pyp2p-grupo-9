import json
import socket

class Packet:
    """Cria, envia e armazena packets. Contém métodos para criar packets específicos ou arbitrários."""
    def __init__(self):
        self.type = "none"
        self.content = json.dumps("{}", indent=4)

    def create(self, type : str, **kwargs):
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
        json_packet = json.dumps(packet)
        self.content = json_packet
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
        sock.settimeout(5)


        try:
            sock.connect((host, port))
            #sock.sendall(bytes(self.content, encoding="utf-8"))
            #received = sock.recv(1024)
            #received = received.decode("utf-8")

            # IMPORTANTE: o servidor rendezvous espera JSON em UMA LINHA terminada por '\n'
            data = (self.content + "\n").encode("utf-8")
            sock.sendall(data)

            received = b""
            # lê até achar um '\n' ou acabar a conexão
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                received += chunk
                if b"\n" in chunk:
                    break

            received_str = received.decode("utf-8").strip()



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
            
