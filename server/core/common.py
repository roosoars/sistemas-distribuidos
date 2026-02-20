"""
Utilitarios comuns do sistema distribuido.

Responsabilidade unica: funcoes auxiliares compartilhadas.
"""

import logging
import sys


def setup_logging(name: str) -> logging.Logger:
    """Configura logging padronizado para o sistema."""
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [{name.upper()}] %(levelname)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(name)


# Re-exporta VoteState do novo modulo para compatibilidade
from core.vote_state import VoteState, VoteEntry

__all__ = ['setup_logging', 'VoteState', 'VoteEntry']
