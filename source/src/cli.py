import sys
import time
import uuid
import datetime
import json

from typing import Optional, List

from cliente_p2p import ClienteP2P as Connection
from packetman import Packet


def print_help() -> None:
    print("""
Comandos disponíveis:

  /register
      Registra este peer no servidor Rendezvous.

  /discover [namespace]
      Descobre peers registrados. Se namespace for omitido, faz discover global.

  /peers [refresh]
      Mostra a última lista de peers descobertos.
      Se passar 'refresh', faz um discover no seu namespace antes de mostrar.

  /listen [port]
      Inicia o servidor TCP local para aceitar conexões de outros peers.
      Se a porta não for informada, usa a porta de peer_info.json.

  /hello <ip> <port>
      Faz handshake HELLO com um peer específico (ip, porta).

  /ping <ip> <port>
      Envia PING para um peer (ip, porta) e mostra o RTT.

  /msg <peer_id> <mensagem>
      Envia uma mensagem unicast (SEND+ACK) para um peer específico.
      peer_id normalmente é no formato nome@namespace (ex: ana@unb).

  /pub <destino> <mensagem>
      Publica mensagem para múltiplos peers.
      destino pode ser:
        *  -> todos os peers descobertos
        #ns -> todos os peers de um namespace (ex: #unb)

  /rtt
      Mostra os RTTs médios registrados por peer (via /msg e /ping).
      
  /conn
        Mostra as conexões TCP ativas com outros peers.    

  /help
      Mostra esta ajuda.
      
/bye <peer_id | indice>
      Encerra a conexão persistente com um peer específico.

  /quit ou /exit
      Sai do programa.
""")
    
def _resolver_peer_id_persistente(conn: Connection, alvo: str) -> Optional[str]:
    """
    Resolve um peer a partir de um alvo (índice da /peers ou peer_id direto)
    e garante que exista uma conexão persistente (ConexaoPeer) para ele.

    Retorna:
      - peer_id (string) se existir conexão persistente
      - None caso contrário (e imprime mensagem de erro).
    """
    peer_id: Optional[str] = None

    # Caso 1: alvo é um índice numérico da lista /peers
    if alvo.isdigit():
        idx = int(alvo)
        if idx < 1 or idx > len(conn.latestDiscover):
            print(f"[PEER] Índice {idx} fora da faixa da lista de peers. Use /peers para conferir.")
            return None

        p = conn.latestDiscover[idx - 1]
        name = p.get("name") or "?"
        ns = p.get("namespace") or "?"
        peer_id = p.get("peer_id") or f"{name}@{ns}"
    else:
        # Caso 2: alvo é um peer_id explícito (ex: Grupo-9@UnB)
        peer_id = alvo

    # Verifica se há conexão persistente para esse peer_id
    conexao = conn.conexoes.get(peer_id)
    if conexao is None:
        print(
            f"[PEER] Não há conexão persistente para '{peer_id}'. "
            "Use /hello <ip> <porta> primeiro para estabelecer a conexão."
        )
        return None

    return peer_id



def find_peer_in_discover(conn: Connection, peer_id: str) -> Optional[dict]:
    """
    Procura um peer na lista latestDiscover do Connection.

    - Tenta primeiro pelo campo "peer_id".
    - Depois tenta montar peer_id como "name@namespace".
    """
    for p in conn.latestDiscover:
        # 1) campo explícito "peer_id"
        if p.get("peer_id") == peer_id:
            return p

        # 2) combina name@namespace
        name = p.get("name")
        ns = p.get("namespace")
        if name and ns and f"{name}@{ns}" == peer_id:
            return p

    return None


def cmd_ping(conn: Connection, args: List[str]) -> None:
    """
    Envia PING para um peer via conexão persistente e tenta medir o RTT.

    Sintaxe:
      /ping <peer_id | indice>

    - Exige que exista ConexaoPeer para o peer (via /hello).
    - O RTT é medido a partir da atualização feita pela ConexaoPeer
      na TabelaPeers quando o PONG chega.
    """
    if len(args) != 1:
        print("Uso: /ping <peer_id | indice>")
        return

    alvo = args[0]

    # 1) Resolver peer_id e garantir conexão persistente
    peer_id = _resolver_peer_id_persistente(conn, alvo)
    if peer_id is None:
        return

    conexao = conn.conexoes.get(peer_id)
    if conexao is None:
        print(
            f"[PING] Não há conexão persistente para '{peer_id}'. "
            "Use /hello <ip> <porta> antes."
        )
        return

    # 2) Guardar RTT antigo (se existir) para detectar mudança
    entry_before = conn.tabela_peers.obter(peer_id)
    rtt_antigo = entry_before.ultimo_rtt if entry_before else None

    # 3) Enviar PING via conexão persistente
    msg_id = f"cli-{time.monotonic_ns()}"
    print(f"[PING] Enviando PING para {peer_id} via conexão persistente (msg_id={msg_id})...")
    ok = conexao.enviar_ping(msg_id)
    if not ok:
        print(f"[PING] Falha ao enviar PING para {peer_id}.")
        return

    # 4) Esperar um tempo para o PONG chegar e a TabelaPeers ser atualizada
    timeout_segundos = 3.0
    inicio = time.perf_counter()
    while time.perf_counter() - inicio < timeout_segundos:
        time.sleep(0.05)
        entry = conn.tabela_peers.obter(peer_id)
        if entry and entry.ultimo_rtt is not None:
            # Se não havia RTT antes, qualquer valor serve.
            # Se já havia, consideramos mudança se for significativamente diferente.
            if rtt_antigo is None or abs(entry.ultimo_rtt - rtt_antigo) > 0.001:
                rtt = entry.ultimo_rtt
                print(f"[PING] PONG recebido de {peer_id}. RTT ≈ {rtt:.2f} ms.")
                # Mantém compatibilidade com conn.rtt
                conn.rtt[peer_id] = rtt
                return

    print(
        f"[PING] PING enviado para {peer_id}, "
        "mas não foi possível medir RTT dentro do timeout. "
        "Verifique em /conn se a conexão ainda está ATIVA."
    )



def cmd_msg(conn: Connection, args: List[str]) -> None:
    """
    Envia uma mensagem de texto para um peer VIA CONEXÃO PERSISTENTE.

    Sintaxe:
      /msg <peer_id | indice> <mensagem>

    - <indice> é a posição mostrada em /peers.
    - <peer_id> é o identificador do tipo "Grupo-9@UnB".
    - É OBRIGATÓRIO já existir uma conexão persistente (via /hello).
    """
    if len(args) < 2:
        print("Uso: /msg <peer_id | indice> <mensagem>")
        return

    alvo = args[0]
    mensagem = " ".join(args[1:])

    # 1) Descobrimos qual peer_id é e garantimos que há ConexaoPeer
    peer_id = _resolver_peer_id_persistente(conn, alvo)
    if peer_id is None:
        return

    conexao = conn.conexoes.get(peer_id)
    if conexao is None:
        # Teoricamente já tratado no helper, mas deixo o guard aqui por segurança
        print(
            f"[MSG] Não há conexão persistente para '{peer_id}'. "
            "Use /hello <ip> <porta> antes."
        )
        return

    # 2) Envia SEND pela conexão persistente
    ok = conexao.enviar_send(mensagem, dst_peer_id=peer_id)
    if ok:
        print(f"[MSG] Mensagem enviada para {peer_id} via conexão persistente.")
    else:
        print(f"[MSG] Falha ao enviar mensagem para {peer_id} pela conexão persistente.")




def cmd_pub(conn: Connection, dst: str, message: str) -> None:
    """
    Implementa /pub destino mensagem usando APENAS conexões persistentes.

    destino:
      *   -> todos os peers em latestDiscover
      #ns -> todos os peers do namespace ns

    Regra:
      - Só envia para peers que já têm ConexaoPeer ativa em conn.conexoes.
      - Se não houver conexão persistente, avisa para usar /hello antes.
    """
    if not conn.latestDiscover:
        print("[PUB] Nenhum peer conhecido. Rode /discover antes.")
        return

    if dst == "*":
        targets = conn.latestDiscover
    elif dst.startswith("#"):
        ns = dst[1:]
        targets = [p for p in conn.latestDiscover if p.get("namespace") == ns]
    else:
        print("[PUB] Destino inválido. Use '*' ou '#<namespace>'.")
        return

    if not targets:
        print(f"[PUB] Nenhum peer corresponde ao destino '{dst}'.")
        return

    print(f"[PUB] Publicando mensagem '{message}' para até {len(targets)} peers (destino={dst})")

    enviados = 0
    pulados_sem_conexao = 0

    for p in targets:
        name = p.get("name") or "?"
        ns = p.get("namespace") or "?"
        peer_id = p.get("peer_id") or f"{name}@{ns}"

        conexao = conn.conexoes.get(peer_id)
        if conexao is None:
            pulados_sem_conexao += 1
            print(
                f"  -> [SKIP] {peer_id}: sem conexão persistente. "
                "Use /hello <ip> <porta> antes."
            )
            continue

        ok = conexao.enviar_pub(message, dst=dst)
        status = "OK" if ok else "FALHA"
        if ok:
            enviados += 1
        print(f"  -> [{status}] (persistente) {peer_id}")

    print(
        f"[PUB] Conclusão: enviados={enviados}, "
        f"sem_conexao={pulados_sem_conexao}, total_alvos={len(targets)}"
    )

        
def cmd_bye(conn: Connection, args: List[str]) -> None:
    """
    Encerra de forma graciosa UMA conexão persistente com um peer.

    Sintaxe:
      /bye <peer_id | indice>

    - <indice>: posição mostrada em /peers.
    - <peer_id>: ex: "Grupo-9@UnB".
    """
    if len(args) != 1:
        print("Uso: /bye <peer_id | indice>")
        return

    alvo = args[0]

    # Reaproveita o helper para resolver peer_id e verificar se há conexão
    peer_id = _resolver_peer_id_persistente(conn, alvo)
    if peer_id is None:
        return

    conexao = conn.conexoes.get(peer_id)
    if conexao is None:
        print(f"[BYE] Não há conexão persistente para '{peer_id}'.")
        return

    ok = conexao.enviar_bye()
    if ok:
        print(f"[BYE] BYE enviado para {peer_id}. Conexão será encerrada.")
        # Opcional: remover a referência dessa conexão, para o monitor não tentar pingar
        try:
            del conn.conexoes[peer_id]
        except KeyError:
            pass
    else:
        print(f"[BYE] Falha ao enviar BYE para {peer_id}.")



def cmd_rtt(conn: Connection) -> None:
    """
    Implementa /rtt: mostra RTT médio conhecido por peer.
    """
    if not conn.rtt:
        print("[RTT] Nenhuma métrica registrada ainda. Use /ping ou /msg primeiro.")
        return

    print("[RTT] Métricas de RTT por peer:")
    for peer_id, value in conn.rtt.items():
        print(f"  {peer_id}: {value:.2f} ms")
    
def cmd_conn(conn: Connection) -> None:
    """
    Lista as conexões persistentes conhecidas pela TabelaPeers.
    """
    entries = conn.tabela_peers.listar()
    if not entries:
        print("[CONN] Nenhum peer registrado na tabela ainda.")
        return

    print("[CONN] Conexões conhecidas:")
    for e in entries:
        rtt_str = f"{e.ultimo_rtt:.2f} ms" if e.ultimo_rtt is not None else "-"
        ultima = e.ultima_atividade.isoformat() + "Z" if e.ultima_atividade else "-"
        print(
            f"  {e.peer_id:25s} {e.estado:12s} "
            f"{e.host}:{e.porta:<5d} RTT={rtt_str:>10s} Última={ultima}"
        )


def main() -> None:
    conn = Connection()

    print("PyP2p CLI")
    print(f"Peer local: {conn.name}@{conn.namespace} escutando na porta {conn.port}")
    print("Digite /help para ver os comandos.\n")

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSaindo...")
            break

        if not line:
            continue

        if not line.startswith("/"):
            print("Comando inválido. Use /help para ver os comandos.")
            continue

        parts = line.split()
        cmd = parts[0][1:]  # remove o '/'

        # --------------------------------------------------------------
        # Comandos básicos / controle
        # --------------------------------------------------------------
        if cmd in ("quit", "exit"):
            # Encerramento limpo:
            #  1) shutdown do cliente (BYE + sockets + monitor)
            #  2) UNREGISTER no Rendezvous
            #  3) sair da CLI
            try:
                conn.shutdown()
            except Exception as e:
                print(f"[CLI] Erro ao fazer shutdown do cliente: {e}")

            try:
                conn.unregister()
            except Exception as e:
                print(f"[CLI] Erro ao fazer unregister no rendezvous: {e}")

            print("Saindo...")
            break


        elif cmd == "help":
            print_help()

        # --------------------------------------------------------------
        # Integração com Rendezvous
        # --------------------------------------------------------------
        elif cmd == "register":
            conn.register()
            conn.startListen()

        elif cmd == "discover":
            ns: Optional[str] = None
            if len(parts) >= 2:
                ns = parts[1]
            conn.discover(ns)

        elif cmd == "peers":
            refresh = len(parts) >= 2 and parts[1].lower() in ("refresh", "r", "update")
            conn.latestPeers(repeat=refresh)

        # --------------------------------------------------------------
        # Servidor local
        # --------------------------------------------------------------
        elif cmd == "listen":
            if len(parts) >= 2:
                try:
                    port = int(parts[1])
                except ValueError:
                    print("Uso: /listen [porta]")
                    continue
                conn.startListen(port=port)
            else:
                # Usa a porta definida em peer_info.json
                conn.startListen()
                
        # --------------------------------------------------------------
        # Conexões persistentes
        # --------------------------------------------------------------
        elif cmd == "conn":
            cmd_conn(conn)


        # --------------------------------------------------------------
        # Handshake e métricas diretas
        # --------------------------------------------------------------
        elif cmd == "hello":
            if len(parts) != 3:
                print("Uso: /hello <ip> <port>")
                continue
            host = parts[1]
            try:
                port = int(parts[2])
            except ValueError:
                print("Uso: /hello <ip> <port>")
                continue
            conn.hello(host, port)

        elif cmd == "ping":
            # parts[0] = "ping"
            args = parts[1:]  # tudo o que vier depois de 'ping'
            cmd_ping(conn, args)

        # --------------------------------------------------------------
        # Mensagens SEND / PUB
        # --------------------------------------------------------------
        elif cmd == "msg":
            if len(parts) < 3:
                print("Uso: /msg <peer_id | indice> <mensagem>")
                continue
            args = parts[1:]
            cmd_msg(conn, args)

        elif cmd == "pub":
            if len(parts) < 3:
                print("Uso: /pub <destino> <mensagem>")
                print("  destino: *  ou  #<namespace>")
                continue
            dst = parts[1]
            message = " ".join(parts[2:])
            cmd_pub(conn, dst, message)
            
            
        elif cmd == "bye":
            # /bye <peer_id | indice>
            args = parts[1:]
            cmd_bye(conn, args)

        # --------------------------------------------------------------
        # Métricas
        # --------------------------------------------------------------
        elif cmd == "rtt":
            cmd_rtt(conn)

        else:
            print(f"Comando desconhecido: /{cmd}")
            print("Use /help para ver os comandos disponíveis.")


if __name__ == "__main__":
    main()
