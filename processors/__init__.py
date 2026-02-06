"""
Procesadores de remuneraciones para diferentes tipos de subvención.
"""

from processors.base import BaseProcessor, ProcessorError, ProgressCallback
from processors.sep import SEPProcessor
from processors.pie import PIEProcessor
from processors.duplicados import DuplicadosProcessor
from processors.brp import BRPProcessor
from processors.integrado import IntegradoProcessor
from processors.rem import REMProcessor
from processors.anual import AnualProcessor

__all__ = [
    'BaseProcessor',
    'ProcessorError',
    'ProgressCallback',
    'SEPProcessor',
    'PIEProcessor',
    'DuplicadosProcessor',
    'BRPProcessor',
    'IntegradoProcessor',
    'REMProcessor',
    'AnualProcessor',
]
