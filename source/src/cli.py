"""
CLI para o cliente PyP2P.

Comandos implementados neste passo:
  /register                - registra este peer no servidor rendezvous
  /discover                - faz DISCOVER e mostra peers
  /peers                   - mostra os últimos peers descobertos
  /hello <ip> <porta>      - envia HELLO para um peer específico
  /help                    - mostra os comandos disponíveis
  /quit                    - encerra o programa
"""

from comm import Connection  

def print_help():
    print("Comandos disponíveis:")
    print("  /register                - registra este peer no servidor rendezvous")
    print("  /discover                - faz DISCOVER e mostra peers")
    print("  /peers                   - mostra os últimos peers descobertos (sem DISCOVER novo)")
    print("  /hello <ip> <porta>      - envia HELLO para um peer específico")
    print("  /help                    - mostra esta ajuda")
    print("  /quit                    - encerra o programa")
    print()
    print("Observações:")
    print("  - Use /register antes de /discover ou /hello.")
    print("  - /discover atualiza a lista interna de peers da Connection.")
    print()


def main():
    # Instancia o gerenciador de conexão
    conn = Connection()

    print("=== PyP2P CLI ===")
    print(f"Peer local: {conn.name}@{conn.namespace} (porta {conn.port})")
    print_help()

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSaindo...")
            break

        if not line:
            continue

        # Aceita comandos com ou sem barra: "/register" ou "register"
        if line.startswith("/"):
            cmdline = line[1:]
        else:
            cmdline = line

        parts = cmdline.split()
        cmd = parts[0].lower()

        # ===== /register =====
        if cmd == "register":
            conn.register()

        
        # ===== /peers [* | #namespace] =====
        elif cmd == "peers":
            namespace = None
            if len(parts) > 1:
                arg = parts[1]
                if arg == "*":
                    namespace = None  # todos
                elif arg.startswith("#"):
                    namespace = arg[1:]
                else:
                    namespace = arg  # deixa passar direto

            conn.discover(namespace=namespace)
            conn.latestPeers(repeat=False)


        # ===== /hello <ip> <porta> =====
        elif cmd == "hello":
            if len(parts) < 3:
                print("Uso: /hello <ip> <porta>")
                continue

            host = parts[1]
            try:
                port = int(parts[2])
            except ValueError:
                print("Porta inválida. Use um número inteiro, ex: /hello 127.0.0.1 30500")
                continue

            conn.hello(host, port)

        # ===== /help =====
        elif cmd == "help":
            print_help()

        # ===== /quit =====
        elif cmd in ("quit", "exit"):
            print("Encerrando cliente PyP2P...")
            break

                # ===== /msg <peer_id> <mensagem> =====
        elif cmd == "msg":
            if len(parts) < 3:
                print("Uso: /msg <peer_id> <mensagem>")
                continue
            peer_id = parts[1]
            message = " ".join(parts[2:])
            # TODO: integrar com módulo de SEND do grupo
            print(f"[CLI] (stub) Enviaria SEND para {peer_id}: {message}")

        # ===== /pub * <mensagem> =====
        elif cmd == "pub":
            if len(parts) < 3:
                print("Uso: /pub * <mensagem>  ou  /pub #<namespace> <mensagem>")
                continue

            dst = parts[1]
            message = " ".join(parts[2:])

            if dst == "*":
                # TODO: integrar com PUB global
                print(f"[CLI] (stub) Enviaria PUB global: {message}")
            elif dst.startswith("#"):
                namespace = dst[1:]
                # TODO: integrar com PUB para namespace
                print(f"[CLI] (stub) Enviaria PUB para #{namespace}: {message}")
            else:
                print("Destino inválido para /pub. Use * ou #<namespace>.")

        # ===== /conn =====
        elif cmd == "conn":
            # TODO: integrar com PeerTable / conexões do grupo
            print("[CLI] (stub) Listaria conexões ativas aqui.")

        # ===== /rtt =====
        elif cmd == "rtt":
            # TODO: integrar com métricas de RTT da camada de PING/PONG
            print("[CLI] (stub) Mostraria RTT médio por peer aqui.")

        # ===== /reconnect =====
        elif cmd == "reconnect":
            # TODO: integrar com lógica de reconciliação / PeerTable
            print("[CLI] (stub) Forçaria reconciliação de peers aqui.")

        # ===== /log <Nível> =====
        elif cmd == "log":
            if len(parts) < 2:
                print("Uso: /log <Nível> (ex: DEBUG, INFO, WARNING)")
                continue
            level = parts[1].upper()
            # TODO: integrar com módulo de logging
            print(f"[CLI] (stub) Ajustaria nível de log para {level}.")


        # ===== comando desconhecido =====
        else:
            print(f"Comando desconhecido: {cmd}")
            print("Digite /help para ver a lista de comandos.")
        

if __name__ == "__main__":
    main()
