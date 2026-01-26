"""
Configuración de RemuPro.
"""

from config.columns import (
    ColumnConfig,
    SALARY_BENEFIT_COLUMNS,
    SPECIAL_SALARY_COLUMNS,
    WEB_SOSTENEDOR_COLUMNS,
    get_available_columns,
    normalize_rut
)

__all__ = [
    'ColumnConfig',
    'SALARY_BENEFIT_COLUMNS',
    'SPECIAL_SALARY_COLUMNS',
    'WEB_SOSTENEDOR_COLUMNS',
    'get_available_columns',
    'normalize_rut'
]
