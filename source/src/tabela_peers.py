# tabela_peers.py
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


# Estados possíveis de uma conexão com peer
ESTADO_ATIVO = "ATIVO"
ESTADO_CONECTANDO = "CONECTANDO"
ESTADO_STALE = "STALE"
ESTADO_DESCONECTADO = "DESCONECTADO"


@dataclass
class PeerEntry:
    """
    Representa um peer conhecido na rede P2P.

    Aqui guardamos:
      - identificação (peer_id)
      - endereço (host, porta)
      - estado da conexão
      - métricas básicas (RTT, última atividade, falhas consecutivas)
    """
    peer_id: str
    host: str
    porta: int

    estado: str = ESTADO_DESCONECTADO
    ultimo_rtt: Optional[float] = None
    ultima_atividade: Optional[datetime] = None
    falhas_consecutivas: int = 0

    # Campo auxiliar para política de reconexão no futuro
    backoff_segundos: int = 0

    # Campo opcional para associar uma ConexaoPeer (sem type-checking circular)
    conexao: "object | None" = field(default=None, repr=False)


class TabelaPeers:
    """
    Tabela de peers thread-safe.

    - Todas as operações de leitura/escrita passam por um Lock.
    - Pensada para ser usada tanto pela CLI quanto por threads
      de monitoramento (keep-alive / reconexão).
    """

    def __init__(self) -> None:
        self._peers: Dict[str, PeerEntry] = {}
        self._lock = threading.Lock()

    # --------------------------------------------------------------
    # CRUD básico de peers
    # --------------------------------------------------------------

    def registrar_ou_atualizar(self, peer_id: str, host: str, porta: int) -> PeerEntry:
        """
        Cria ou atualiza um registro de peer na tabela.
        """
        with self._lock:
            entry = self._peers.get(peer_id)
            if entry is None:
                entry = PeerEntry(peer_id=peer_id, host=host, porta=int(porta))
                self._peers[peer_id] = entry
            else:
                entry.host = host
                entry.porta = int(porta)
            return entry

    def obter(self, peer_id: str) -> Optional[PeerEntry]:
        with self._lock:
            return self._peers.get(peer_id)

    def listar(self) -> List[PeerEntry]:
        """
        Devolve um snapshot da tabela (lista nova, não mutável externamente).
        """
        with self._lock:
            return list(self._peers.values())

    # --------------------------------------------------------------
    # Atualizações de estado / métricas
    # --------------------------------------------------------------

    def marcar_ativo(self, peer_id: str) -> None:
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.estado = ESTADO_ATIVO
            entry.falhas_consecutivas = 0
            entry.backoff_segundos = 0
            entry.ultima_atividade = datetime.utcnow()

    def marcar_conectando(self, peer_id: str) -> None:
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.estado = ESTADO_CONECTANDO

    def marcar_stale(self, peer_id: str) -> None:
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.estado = ESTADO_STALE

    def marcar_desconectado(self, peer_id: str) -> None:
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.estado = ESTADO_DESCONECTADO

    def atualizar_rtt(self, peer_id: str, rtt: float) -> None:
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.ultimo_rtt = rtt
            entry.ultima_atividade = datetime.utcnow()

    def registrar_falha(self, peer_id: str) -> None:
        """
        Incrementa contador de falhas e ajusta backoff.

        Isso será útil quando implementarmos reconexão automática
        com política do tipo 5s, 10s, 30s, etc.
        """
        with self._lock:
            entry = self._peers.get(peer_id)
            if not entry:
                return
            entry.falhas_consecutivas += 1
            # Exemplo simples de backoff exponencial limitado:
            entry.backoff_segundos = min(60, 5 * (2 ** (entry.falhas_consecutivas - 1)))
            entry.ultima_atividade = datetime.utcnow()
