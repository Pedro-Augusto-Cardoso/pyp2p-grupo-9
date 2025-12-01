# estado.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EstadoPeerLocal:
    """
    Representa a identidade básica do peer local.
    
    """
    nome: str
    namespace: str
    porta_escuta: int


@dataclass
class ConfigRendezvous:
    """
    Configuração do servidor Rendezvous.
    """
    host: str = "localhost" #"pyp2p.mfcaetano.cc"
    porta: int = 8080
    ttl: int = 7200  # segundos
