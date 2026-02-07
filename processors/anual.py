"""
Procesador Anual - Extrae y resume liquidaciones de un archivo anual consolidado.

El archivo anual (~5MB, ~5365 filas, ~200 columnas) contiene TODAS las
liquidaciones del año. Los montos de BRP ya vienen calculados en la columna
'(BRP) Asig. Titulo y' — no se redistribuye, solo se extrae y resume.
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

from config.columns import normalize_rut, clean_columns, classify_contract, parse_periodo
from config.escuelas import match_ubicacion


class AnualProcessor:
    """Procesador de archivos anuales de liquidación consolidados."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def process(
        self, file_path: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict]]:
        """
        Procesa archivo anual y retorna DataFrames de detalle y resumen.

        Args:
            file_path: Ruta al archivo anual (CSV o Excel)

        Returns:
            Tupla (df_mensual, df_resumen_anual, df_escuelas, alertas)
            - df_mensual: detalle mes a mes por docente
            - df_resumen_anual: resumen anual por docente
            - df_escuelas: resumen por escuela y mes
            - alertas: lista de alertas (multi-establecimiento, etc.)
        """
        df = self._load_file(file_path)
        df = self._clean_and_normalize(df)
        df_mensual = self._build_monthly_detail(df)
        df_resumen = self._build_annual_summary(df_mensual)
        df_escuelas = self._build_school_summary(df_mensual)
        alertas = self._detect_multi_establishment(df_mensual)

        self.logger.info(
            f"Anual procesado: {len(df_mensual)} registros mensuales, "
            f"{len(df_resumen)} docentes, {len(df_escuelas)} escuela-mes, "
            f"{len(alertas)} alertas"
        )

        return df_mensual, df_resumen, df_escuelas, alertas

    def _load_file(self, path: Path) -> pd.DataFrame:
        """Carga archivo anual (CSV o Excel)."""
        suffix = path.suffix.lower()
        if suffix == '.csv':
            try:
                df = pd.read_csv(str(path), encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(str(path), encoding='latin-1')
        elif suffix in ('.xlsx', '.xls'):
            df = pd.read_excel(str(path), engine='openpyxl')
        else:
            raise ValueError(f"Formato no soportado: {suffix}. Use CSV o Excel.")

        df = clean_columns(df)

        if df.empty:
            raise ValueError("El archivo anual está vacío.")

        self.logger.info(
            f"Anual cargado: {len(df)} filas, {len(df.columns)} columnas"
        )
        return df

    def _find_col(self, df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
        """Busca una columna por keywords (case-insensitive, substring)."""
        for col in df.columns:
            if not isinstance(col, str):
                continue
            col_lower = col.lower().strip()
            for kw in keywords:
                if kw.lower() in col_lower:
                    return col
        return None

    def _clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza RUT, parsea periodo, clasifica contratos, matchea escuelas."""
        # RUT
        rut_col = self._find_col(df, ['rut'])
        if not rut_col:
            raise ValueError("No se encontró columna 'RUT' en el archivo anual.")
        df['RUT_NORM'] = df[rut_col].apply(normalize_rut)

        # Nombre
        nombre_col = self._find_col(df, ['nombre'])
        if nombre_col:
            df['NOMBRE'] = df[nombre_col].astype(str).str.strip()
        else:
            df['NOMBRE'] = ''

        # Periodo → MES (YYYY-MM)
        periodo_col = self._find_col(df, ['periodo'])
        if periodo_col:
            df['MES'] = df[periodo_col].apply(
                lambda x: parse_periodo(str(x)) or str(x)
            )
        else:
            df['MES'] = 'desconocido'

        # Tipo de contrato → TIPO_SUBVENCION
        tipo_col = self._find_col(df, ['tipo_de_contrato', 'tipocontrato', 'tipo contrato'])
        if tipo_col:
            df['TIPO_SUBVENCION'] = df[tipo_col].apply(classify_contract)
        else:
            df['TIPO_SUBVENCION'] = 'NORMAL'

        # Ubicación → ESCUELA, RBD
        ubicacion_col = self._find_col(df, ['ubicacion', 'ubicación'])
        if ubicacion_col:
            matches = df[ubicacion_col].apply(
                lambda x: match_ubicacion(str(x)) if pd.notna(x) else None
            )
            df['ESCUELA'] = matches.apply(lambda m: m[0] if m else 'DESCONOCIDA')
            df['RBD'] = matches.apply(lambda m: m[1] if m else '')
        else:
            df['ESCUELA'] = 'DESCONOCIDA'
            df['RBD'] = ''

        # Jornada
        jornada_col = self._find_col(df, ['jornada'])
        if jornada_col:
            df['JORNADA'] = pd.to_numeric(df[jornada_col], errors='coerce').fillna(0)
        else:
            df['JORNADA'] = 0

        # Columnas monetarias
        monetary_mappings = [
            (['sueldo base'], 'SUELDO_BASE'),
            (['(brp) asig. titulo', 'brp'], 'BRP'),
            (['total haberes'], 'TOTAL_HABERES'),
            (['liquido neto', 'líquido neto'], 'LIQUIDO_NETO'),
            (['monto imponible'], 'MONTO_IMPONIBLE'),
            (['incentivo (p.i.e)', 'incentivo pie'], 'INCENTIVO_PIE'),
            (['asignacion experienc', 'asignacion experiencia'], 'ASIGNACION_EXPERIENCIA'),
        ]

        for keywords, output_col in monetary_mappings:
            col = self._find_col(df, keywords)
            if col:
                df[output_col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[output_col] = 0

        return df

    def _build_monthly_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        """Construye DataFrame de detalle mensual."""
        cols = [
            'RUT_NORM', 'NOMBRE', 'MES', 'TIPO_SUBVENCION',
            'ESCUELA', 'RBD', 'JORNADA',
            'BRP', 'SUELDO_BASE', 'TOTAL_HABERES', 'LIQUIDO_NETO',
            'MONTO_IMPONIBLE', 'INCENTIVO_PIE', 'ASIGNACION_EXPERIENCIA',
        ]
        available = [c for c in cols if c in df.columns]
        df_mensual = df[available].copy()

        # Agregar por RUT + MES + TIPO + ESCUELA (puede haber duplicados)
        group_cols = ['RUT_NORM', 'NOMBRE', 'MES', 'TIPO_SUBVENCION', 'ESCUELA', 'RBD']
        num_cols = [c for c in available if c not in group_cols]

        agg_dict = {c: 'sum' for c in num_cols if c != 'NOMBRE'}
        agg_dict['NOMBRE'] = 'first'
        group_cols_no_nombre = [c for c in group_cols if c != 'NOMBRE']

        df_mensual = df_mensual.groupby(group_cols_no_nombre, as_index=False).agg(agg_dict)

        self.logger.info(f"Detalle mensual: {len(df_mensual)} registros")
        return df_mensual

    def _build_annual_summary(self, df_mensual: pd.DataFrame) -> pd.DataFrame:
        """Resumen anual por docente: BRP total, meses activos, escuelas."""
        if df_mensual.empty:
            return pd.DataFrame()

        grouped = df_mensual.groupby('RUT_NORM').agg(
            NOMBRE=('NOMBRE', 'first'),
            BRP_TOTAL=('BRP', 'sum'),
            SUELDO_BASE_TOTAL=('SUELDO_BASE', 'sum'),
            HABERES_TOTAL=('TOTAL_HABERES', 'sum'),
            LIQUIDO_TOTAL=('LIQUIDO_NETO', 'sum'),
            MESES_ACTIVOS=('MES', 'nunique'),
            ESCUELAS=('ESCUELA', lambda x: ' | '.join(sorted(x.unique()))),
        ).reset_index()

        grouped['PROMEDIO_MENSUAL'] = (
            grouped['BRP_TOTAL'] / grouped['MESES_ACTIVOS'].replace(0, 1)
        ).round(0)

        self.logger.info(f"Resumen anual: {len(grouped)} docentes")
        return grouped

    def _build_school_summary(self, df_mensual: pd.DataFrame) -> pd.DataFrame:
        """Resumen por escuela y mes."""
        if df_mensual.empty:
            return pd.DataFrame()

        grouped = df_mensual.groupby(['RBD', 'ESCUELA', 'MES']).agg(
            BRP_TOTAL=('BRP', 'sum'),
            HABERES_TOTAL=('TOTAL_HABERES', 'sum'),
            DOCENTES=('RUT_NORM', 'nunique'),
            BRP_SEP=('BRP', lambda x: x[df_mensual.loc[x.index, 'TIPO_SUBVENCION'] == 'SEP'].sum()),
            BRP_PIE=('BRP', lambda x: x[df_mensual.loc[x.index, 'TIPO_SUBVENCION'] == 'PIE'].sum()),
            BRP_NORMAL=('BRP', lambda x: x[df_mensual.loc[x.index, 'TIPO_SUBVENCION'] == 'NORMAL'].sum()),
            BRP_EIB=('BRP', lambda x: x[df_mensual.loc[x.index, 'TIPO_SUBVENCION'] == 'EIB'].sum()),
        ).reset_index()

        self.logger.info(f"Resumen escuelas: {len(grouped)} registros escuela-mes")
        return grouped

    def _detect_multi_establishment(self, df_mensual: pd.DataFrame) -> List[Dict]:
        """Detecta docentes en 2+ establecimientos en el año."""
        alertas = []
        if df_mensual.empty:
            return alertas

        # RBDs por docente (ignorar vacíos y DEM)
        df_filtered = df_mensual[
            (df_mensual['RBD'] != '') & (df_mensual['RBD'] != 'DEM')
        ]
        rbds_per_teacher = df_filtered.groupby('RUT_NORM')['RBD'].nunique()
        multi = rbds_per_teacher[rbds_per_teacher >= 2]

        for rut in multi.index:
            teacher_data = df_filtered[df_filtered['RUT_NORM'] == rut]
            nombre = teacher_data['NOMBRE'].iloc[0] if 'NOMBRE' in teacher_data.columns else ''
            escuelas = teacher_data.groupby(['RBD', 'ESCUELA'])['MES'].apply(
                lambda x: sorted(x.unique())
            ).reset_index()

            establecimientos = []
            for _, row in escuelas.iterrows():
                establecimientos.append({
                    'rbd': row['RBD'],
                    'escuela': row['ESCUELA'],
                    'meses': row['MES'],
                })

            alertas.append({
                'tipo': 'multi_establecimiento_anual',
                'rut': rut,
                'nombre': nombre,
                'num_establecimientos': int(multi[rut]),
                'establecimientos': establecimientos,
            })

        if alertas:
            self.logger.warning(
                f"Anual: {len(alertas)} docentes en múltiples establecimientos"
            )

        return alertas
