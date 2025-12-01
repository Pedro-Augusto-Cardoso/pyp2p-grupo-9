# conexao_rendezvous.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from packetman import Packet
from estado import EstadoPeerLocal, ConfigRendezvous


class ConexaoRendezvous:
    """
    Encapsula a comunicação de alto nível com o servidor Rendezvous.

    Aqui ficam:
      - REGISTER
      - DISCOVER (global ou por namespace)
      - UNREGISTER (caso o servidor suporte)

    Essa classe NÃO sabe nada de CLI nem de sockets de peer-to-peer;
    ela só fala com o Rendezvous.
    """

    def __init__(
        self,
        estado_peer: EstadoPeerLocal,
        cfg: Optional[ConfigRendezvous] = None,
    ) -> None:
        self.estado_peer = estado_peer
        self.cfg = cfg or ConfigRendezvous()

        # Última lista de peers retornada pelo DISCOVER
        self.ultima_lista: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # REGISTER / UNREGISTER
    # ------------------------------------------------------------------

    def registrar(self) -> None:
        """
        Envia um REGISTER para o Rendezvous usando os dados do peer local.
        """
        pkt = Packet().create(
            "REGISTER",
            namespace=self.estado_peer.namespace,
            name=self.estado_peer.nome,
            port=self.estado_peer.porta_escuta,
            ttl=self.cfg.ttl,
        )

        try:
            resp = pkt.send(self.cfg.host, self.cfg.porta)
        except OSError as e:
            print(f"[REGISTER] Erro ao conectar no rendezvous: {e}")
            return

        if not resp:
            print("[REGISTER] Sem resposta do servidor rendezvous.")
            return

        if resp.get("status") == "OK":
            # Se o servidor devolver um TTL “ajustado”, atualizamos
            ttl_resp = resp.get("ttl")
            if isinstance(ttl_resp, int):
                self.cfg.ttl = ttl_resp
            print("[REGISTER] Registro realizado com sucesso!")
        else:
            print("[REGISTER] Erro de rendezvous, resposta recebida:")
            print(json.dumps(resp, indent=2, ensure_ascii=False))

    def remover_registro(self) -> None:
        """
        Opcional: tenta fazer UNREGISTER no Rendezvous.
        Se o servidor não suportar, apenas loga o erro.
        """
        pkt = Packet().create(
            "UNREGISTER",
            namespace=self.estado_peer.namespace,
            name=self.estado_peer.nome,
            port=self.estado_peer.porta_escuta,
        )

        try:
            resp = pkt.send(self.cfg.host, self.cfg.porta)
        except OSError as e:
            print(f"[UNREGISTER] Erro ao conectar no rendezvous: {e}")
            return

        if not resp:
            print("[UNREGISTER] Sem resposta do servidor rendezvous.")
            return

        status = resp.get("status")
        if status == "OK":
            print("[UNREGISTER] Registro removido com sucesso.")
        else:
            print("[UNREGISTER] Resposta inesperada do rendezvous:")
            print(json.dumps(resp, indent=2, ensure_ascii=False))

    # ------------------------------------------------------------------
    # DISCOVER
    # ------------------------------------------------------------------

    def discover(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Faz DISCOVER no Rendezvous (global ou limitado a um namespace).

        Atualiza self.ultima_lista e devolve a lista para o chamador.
        """
        kwargs: Dict[str, Any] = {}
        if namespace:
            kwargs["namespace"] = namespace

        pkt = Packet().create("DISCOVER", **kwargs)

        try:
            resp = pkt.send(self.cfg.host, self.cfg.porta)
        except OSError as e:
            print(f"[DISCOVER] Erro ao conectar no rendezvous: {e}")
            self.ultima_lista = []
            return []

        if not resp:
            print("[DISCOVER] Sem resposta do servidor rendezvous.")
            self.ultima_lista = []
            return []

        status = resp.get("status")
        if status != "OK":
            print("[DISCOVER] Erro de rendezvous, resposta recebida:")
            print(json.dumps(resp, indent=2, ensure_ascii=False))
            self.ultima_lista = []
            return []

        peers = resp.get("peers", [])
        if not isinstance(peers, list):
            print("[DISCOVER] Resposta malformada: campo 'peers' não é lista.")
            self.ultima_lista = []
            return []

        self.ultima_lista = peers
        print(f"[DISCOVER] {len(peers)} peer(s) encontrados.")
        return peers
