"""
Módulo de reportes para RemuPro.
Incluye sistema de auditoría y generación de informes Word.
"""

from reports.audit_log import AuditLog, AuditEntry
from reports.word_report import InformeWord

__all__ = [
    'AuditLog',
    'AuditEntry',
    'InformeWord',
]
