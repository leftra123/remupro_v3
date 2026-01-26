"""
Configuración centralizada de columnas para procesamiento de remuneraciones.
Esto evita duplicación y facilita el mantenimiento.
"""

from dataclasses import dataclass, field
from typing import List, Dict, FrozenSet

@dataclass(frozen=True)
class ColumnConfig:
    """Configuración inmutable de columnas para procesadores."""
    
    # Columnas requeridas por hoja
    REQUIRED_HORAS: FrozenSet[str] = frozenset({'Rut', 'Nombre'})
    REQUIRED_TOTAL: FrozenSet[str] = frozenset({'Rut'})
    
    # Columnas de horas específicas por tipo
    SEP_HOURS_COL: str = 'SEP'
    PIE_HOURS_COL: str = 'PIE'
    SN_HOURS_COL: str = 'SN'
    
    # Límite máximo de horas permitidas
    MAX_HOURS: int = 44


# Columnas especiales que requieren cálculo diferenciado
SPECIAL_SALARY_COLUMNS: List[str] = [
    'SUELDO BASE',
    'RBMN (SUELDO BASE)',
    'ASIGNACION EXPERIENCIA',
    'Antic SEG.INV.SOB.',
    'SEG.CESANTIA EMP.',
    'MUTUAL'
]

# Columnas de salarios y beneficios para prorrateo
SALARY_BENEFIT_COLUMNS: List[str] = [
    # Asignaciones principales
    'ASIGNACION RESPONSABILIDAD', 'CONDICION DIFICIL', 'COMPLEMENTO DE ZONA',
    '(BRP) Asig. Titulo y M', 'PROF. ENCARGADO LEY.', 'HORAS EXTRAS RETROACT.',
    'ASIGNACION ESPECIAL', 'ASIG.RESP. UTP', 'HORAS EXTRAS DEM', 'RETRO.FAMILIAR',
    'BONO VACACIONES', 'PAGO RETROACTIVO', 'LEY 19464/96', 'BRP RETROAC/REEMPL.',
    'BONIFICACION ESPECIAL', 'INCENTIVO (P.I.E)', 'EXCELENCIA ACADEMICA',
    'ASIG. TITULO ESPECIAL', 'DEVOLUCION DESCUENTO', 'RETROBONO INCENTIVO',
    'ASIG. FAMILIAR CORR.', 'BONO CUMPLIMIENTO METAS', 'ASIG.DIRECTOR.LEY 20501',
    'RESP. INSPECTOR GENERAL', 'COND.DIFICIL.ASIST.EDUCACIÓN',
    'ASIGNACION LEY 20.501/2011 DIR', 'RETROACTIVO BIENIOS',
    'RETROACTIVO PROFESOR ENCARGADO', 'ASIG.RESPONS. 6HRS',
    'RETROCT.ALS.PRIORIT.ASIST.EDUC', 'ART.59 LEY 20.883BONO ASISTEDU',
    'RETROACT.ASIGN.RESPOS.DIRECTIV', 'ALS PRIORIT.ASIST.EDUC.AÑO2022',
    'LEY 21.405 ART.44  ASISTE.EDUC', 'ASIGNACION INDUCCION CPEIP',
    'AJUSTE BONO LEY 20.883ART59  A', 'RESTITUCION LICEN.MEDICA',
    'ART.42 LEY 21.526 ASIST.EDUC', 'ALUMNOS. PRIORITARIOS ASIS. DE',
    'ASIG.Por Tramo de Desarrollo P', 'Rec. Doc. Establ. Als Priorita',
    'Planilla Suplementaria', 'ART.5°TRANS. LEY20.903',
    
    # Totales y descuentos
    '  TOTAL HABERES', '  IMPOSICIONES antic', '  SALUD',
    '  Imposicion Voluntaria', '  MONTO IMPONIBLE', '  MONTO IMP.DESAHUCIO',
    '  IMPUESTO UNICO', '  MONTO TRIBUTABLE', '  DIA NO TRABAJADO',
    '  RET. JUDICIAL', '  A.P.V', '  SEGURO DE CESANTIA',
    '  HDI CIA. DE SEGUROS', '  HDI CONDUCTORES', '  AGRUPACION CODOCENTE',
    '  TEMUCOOP (COOPERATIVA DE AHO', '  COOPAHOCRED.KUMEMOGEN LTDA',
    '  CRED. COOPEUCH BIENESTAR', '  PRESTAMO/ACCIONES- COOPEUCH',
    '  MUTUAL DE SEGUROS DE CHILE', '  1% PROFESORES DE RELIGION',
    '  CUOTA BIENESTAR 1%', '  CHILENA CONSOLIDADA - SEGURO', '  ATRASOS',
    '  VIDA SECURITY - SEGUROS DE V', '  BIENESTAR CUOTA INCORP. CUO',
    '  REINTEGRO', '  CAJA LOS ANDES - SEGUROS Y P', '  CAJA LOS ANDES - AHORRO',
    '  COLEGIO PROFESORES 1%', '  APORTE SEG. INV. SOB.', '  REINTEGRO BIENIO',
    '  1% ASOC.AGFAE', '  AHORRO AFP', '  RETENCION POR LICEN. MEDICA',
    '  BONO DOCENTE', '  SEGURO DE CESANTIA', '  SEGURO FALP',
    '  COLEGIO PROFESORES 1% HABER', '  Ajuste IMPOSICIONES'
]

# Columnas del archivo web_sostenedor (MINEDUC)
WEB_SOSTENEDOR_COLUMNS: Dict[str, str] = {
    'rbd': 'Rbd (Establecimiento)',
    'rut': 'RUT (Docente)',
    'nombres': 'Nombres (Docente)',
    'apellido1': 'Primer Apellido (Docente)',
    'apellido2': 'Segundo Apellido (Docente)',
    'bienios': 'Bienios',
    'tramo': 'Tramo',
    'carrera': 'Carrera docente',
    'horas_contrato': 'Horas de contrato',
    'dias_trabajados': 'Total días trabajados o descontados',
    'subvencion_titulo': 'Subvención título',
    'transferencia_titulo': 'Transferencia directa título',
    'subvencion_mencion': 'Subvención mención',
    'transferencia_mencion': 'Transferencia directa mención',
    'total_subv_reconocimiento': 'Total subvención reconocimiento profesional',
    'total_transf_reconocimiento': 'Total transferencia directa reconocimiento',
    'total_reconocimiento': 'Total reconocimiento profesional',
    'subvencion_tramo': 'Subvención tramo',
    'transferencia_tramo': 'Transferencia directa tramo',
    'total_tramo': 'Total tramo',
    'asig_prioritarios': 'Asignación directa alumnos prioritarios',
    'total_subvenciones': 'Total subvenciones',
    'total_transferencia': 'Total transferencia directa',
    'porcentaje_prioritarios': 'Porcentaje Alumnos Prioritarios',
    'sep': 'SEP',
    'pie': 'PIE',
    'general': 'GENERAL'
}


def get_available_columns(df, column_list: List[str]) -> List[str]:
    """Retorna solo las columnas que existen en el DataFrame."""
    return [col for col in column_list if col in df.columns]


def normalize_rut(rut) -> str:
    """Normaliza un RUT chileno removiendo puntos y guiones."""
    if rut is None or (isinstance(rut, float) and str(rut) == 'nan'):
        return ''
    return str(rut).strip().upper().replace('.', '').replace('-', '').replace(' ', '')
