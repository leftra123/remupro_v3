"""
Comparador de meses para análisis de cambios entre períodos.
"""

from typing import Dict, List, Any, Optional
import pandas as pd
from database.repository import BRPRepository


class ComparadorMeses:
    """
    Compara datos de procesamiento entre dos meses.

    Identifica docentes nuevos, salientes, cambios de monto,
    cambios de establecimiento y variaciones en horas.
    """

    # Umbral para considerar un cambio de monto como significativo
    UMBRAL_CAMBIO_PORCENTAJE = 10.0  # 10%

    def __init__(self, repository: BRPRepository):
        """
        Inicializa el comparador.

        Args:
            repository: Repositorio de datos BRP
        """
        self.repo = repository

    def comparar(
        self,
        mes_anterior: str,
        mes_actual: str
    ) -> Dict[str, Any]:
        """
        Compara dos meses y genera reporte de cambios.

        Args:
            mes_anterior: Mes base de comparación
            mes_actual: Mes actual a comparar

        Returns:
            Diccionario con todos los cambios detectados
        """
        df_anterior = self.repo.obtener_datos_mes(mes_anterior)
        df_actual = self.repo.obtener_datos_mes(mes_actual)

        if df_anterior.empty or df_actual.empty:
            return self._empty_result()

        # Obtener conjuntos de RUTs
        ruts_anterior = set(df_anterior['rut'].unique())
        ruts_actual = set(df_actual['rut'].unique())

        # Docentes nuevos y salientes
        ruts_nuevos = ruts_actual - ruts_anterior
        ruts_salieron = ruts_anterior - ruts_actual
        ruts_comunes = ruts_anterior & ruts_actual

        # Preparar DataFrames indexados por RUT
        df_ant_idx = df_anterior.set_index('rut')
        df_act_idx = df_actual.set_index('rut')

        # Analizar cambios
        cambios_montos = self._detectar_cambios_montos(
            df_ant_idx, df_act_idx, ruts_comunes
        )
        cambios_rbd = self._detectar_cambios_rbd(
            df_ant_idx, df_act_idx, ruts_comunes
        )
        cambios_horas = self._detectar_cambios_horas(
            df_ant_idx, df_act_idx, ruts_comunes
        )

        # Resumen estadístico
        resumen_anterior = self.repo.obtener_resumen_mes(mes_anterior)
        resumen_actual = self.repo.obtener_resumen_mes(mes_actual)

        resumen = self._generar_resumen(
            resumen_anterior, resumen_actual,
            len(ruts_nuevos), len(ruts_salieron),
            len(cambios_montos), len(cambios_rbd)
        )

        return {
            'mes_anterior': mes_anterior,
            'mes_actual': mes_actual,
            'docentes_nuevos': self._obtener_info_docentes(
                df_actual, ruts_nuevos
            ),
            'docentes_salieron': self._obtener_info_docentes(
                df_anterior, ruts_salieron
            ),
            'cambios_montos': cambios_montos,
            'cambios_rbd': cambios_rbd,
            'cambios_horas': cambios_horas,
            'resumen': resumen
        }

    def _empty_result(self) -> Dict[str, Any]:
        """Retorna resultado vacío."""
        return {
            'docentes_nuevos': [],
            'docentes_salieron': [],
            'cambios_montos': [],
            'cambios_rbd': [],
            'cambios_horas': [],
            'resumen': {}
        }

    def _detectar_cambios_montos(
        self,
        df_ant: pd.DataFrame,
        df_act: pd.DataFrame,
        ruts_comunes: set
    ) -> List[Dict]:
        """Detecta cambios significativos en montos BRP."""
        cambios = []

        for rut in ruts_comunes:
            try:
                monto_ant = df_ant.loc[rut, 'brp_total']
                monto_act = df_act.loc[rut, 'brp_total']

                # Manejar series si hay múltiples entradas
                if isinstance(monto_ant, pd.Series):
                    monto_ant = monto_ant.sum()
                if isinstance(monto_act, pd.Series):
                    monto_act = monto_act.sum()

                if monto_ant > 0:
                    cambio_pct = ((monto_act - monto_ant) / monto_ant) * 100
                elif monto_act > 0:
                    cambio_pct = 100  # De 0 a algo
                else:
                    continue

                if abs(cambio_pct) >= self.UMBRAL_CAMBIO_PORCENTAJE:
                    nombre_ant = df_ant.loc[rut, 'nombre']
                    if isinstance(nombre_ant, pd.Series):
                        nombre_ant = nombre_ant.iloc[0]

                    cambios.append({
                        'rut': rut,
                        'nombre': nombre_ant,
                        'monto_anterior': monto_ant,
                        'monto_actual': monto_act,
                        'diferencia': monto_act - monto_ant,
                        'cambio_porcentaje': round(cambio_pct, 1)
                    })
            except (KeyError, TypeError):
                continue

        # Ordenar por magnitud de cambio
        cambios.sort(key=lambda x: abs(x['cambio_porcentaje']), reverse=True)
        return cambios

    def _detectar_cambios_rbd(
        self,
        df_ant: pd.DataFrame,
        df_act: pd.DataFrame,
        ruts_comunes: set
    ) -> List[Dict]:
        """Detecta cambios de establecimiento."""
        cambios = []

        for rut in ruts_comunes:
            try:
                rbd_ant = df_ant.loc[rut, 'rbd']
                rbd_act = df_act.loc[rut, 'rbd']

                # Manejar series
                if isinstance(rbd_ant, pd.Series):
                    rbd_ant = set(rbd_ant.unique())
                else:
                    rbd_ant = {rbd_ant}

                if isinstance(rbd_act, pd.Series):
                    rbd_act = set(rbd_act.unique())
                else:
                    rbd_act = {rbd_act}

                if rbd_ant != rbd_act:
                    nombre = df_ant.loc[rut, 'nombre']
                    if isinstance(nombre, pd.Series):
                        nombre = nombre.iloc[0]

                    cambios.append({
                        'rut': rut,
                        'nombre': nombre,
                        'rbd_anterior': ', '.join(str(r) for r in rbd_ant),
                        'rbd_actual': ', '.join(str(r) for r in rbd_act)
                    })
            except (KeyError, TypeError):
                continue

        return cambios

    def _detectar_cambios_horas(
        self,
        df_ant: pd.DataFrame,
        df_act: pd.DataFrame,
        ruts_comunes: set
    ) -> List[Dict]:
        """Detecta cambios en distribución de horas."""
        cambios = []

        for rut in ruts_comunes:
            try:
                # Obtener horas anteriores
                sep_ant = df_ant.loc[rut, 'horas_sep'] if 'horas_sep' in df_ant.columns else 0
                pie_ant = df_ant.loc[rut, 'horas_pie'] if 'horas_pie' in df_ant.columns else 0
                sn_ant = df_ant.loc[rut, 'horas_sn'] if 'horas_sn' in df_ant.columns else 0

                # Obtener horas actuales
                sep_act = df_act.loc[rut, 'horas_sep'] if 'horas_sep' in df_act.columns else 0
                pie_act = df_act.loc[rut, 'horas_pie'] if 'horas_pie' in df_act.columns else 0
                sn_act = df_act.loc[rut, 'horas_sn'] if 'horas_sn' in df_act.columns else 0

                # Sumar si hay múltiples filas
                for var in [sep_ant, pie_ant, sn_ant, sep_act, pie_act, sn_act]:
                    if isinstance(var, pd.Series):
                        var = var.sum()

                if isinstance(sep_ant, pd.Series):
                    sep_ant = sep_ant.sum()
                if isinstance(pie_ant, pd.Series):
                    pie_ant = pie_ant.sum()
                if isinstance(sn_ant, pd.Series):
                    sn_ant = sn_ant.sum()
                if isinstance(sep_act, pd.Series):
                    sep_act = sep_act.sum()
                if isinstance(pie_act, pd.Series):
                    pie_act = pie_act.sum()
                if isinstance(sn_act, pd.Series):
                    sn_act = sn_act.sum()

                # Verificar si hay cambio significativo
                diff_sep = abs(sep_act - sep_ant)
                diff_pie = abs(pie_act - pie_ant)
                diff_sn = abs(sn_act - sn_ant)

                if diff_sep > 0 or diff_pie > 0 or diff_sn > 0:
                    nombre = df_ant.loc[rut, 'nombre']
                    if isinstance(nombre, pd.Series):
                        nombre = nombre.iloc[0]

                    cambios.append({
                        'rut': rut,
                        'nombre': nombre,
                        'sep_anterior': sep_ant,
                        'sep_actual': sep_act,
                        'pie_anterior': pie_ant,
                        'pie_actual': pie_act,
                        'sn_anterior': sn_ant,
                        'sn_actual': sn_act
                    })
            except (KeyError, TypeError):
                continue

        return cambios

    def _obtener_info_docentes(
        self,
        df: pd.DataFrame,
        ruts: set
    ) -> List[Dict]:
        """Obtiene información básica de un conjunto de docentes."""
        info = []
        for rut in ruts:
            try:
                rows = df[df['rut'] == rut]
                if rows.empty:
                    continue

                row = rows.iloc[0]
                info.append({
                    'rut': rut,
                    'nombre': row.get('nombre', ''),
                    'rbd': row.get('rbd', ''),
                    'brp_total': row.get('brp_total', 0)
                })
            except (KeyError, TypeError):
                continue

        return info

    def _generar_resumen(
        self,
        resumen_ant: Optional[Dict],
        resumen_act: Optional[Dict],
        nuevos: int,
        salieron: int,
        cambios_monto: int,
        cambios_rbd: int
    ) -> Dict[str, Any]:
        """Genera resumen estadístico de la comparación."""
        if not resumen_ant or not resumen_act:
            return {}

        brp_ant = resumen_ant.get('brp_total', 0)
        brp_act = resumen_act.get('brp_total', 0)
        diff_brp = brp_act - brp_ant
        pct_brp = (diff_brp / brp_ant * 100) if brp_ant > 0 else 0

        doc_ant = resumen_ant.get('total_docentes', 0)
        doc_act = resumen_act.get('total_docentes', 0)

        return {
            'docentes_anterior': doc_ant,
            'docentes_actual': doc_act,
            'docentes_nuevos': nuevos,
            'docentes_salieron': salieron,
            'brp_anterior': brp_ant,
            'brp_actual': brp_act,
            'diferencia_brp': diff_brp,
            'cambio_brp_pct': round(pct_brp, 1),
            'cambios_monto_significativo': cambios_monto,
            'cambios_establecimiento': cambios_rbd,
            'sep_anterior': resumen_ant.get('brp_sep', 0),
            'sep_actual': resumen_act.get('brp_sep', 0),
            'pie_anterior': resumen_ant.get('brp_pie', 0),
            'pie_actual': resumen_act.get('brp_pie', 0),
            'normal_anterior': resumen_ant.get('brp_normal', 0),
            'normal_actual': resumen_act.get('brp_normal', 0)
        }

    def generar_reporte_comparacion(
        self,
        comparacion: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Genera DataFrame con reporte de comparación.

        Args:
            comparacion: Resultado de comparar()

        Returns:
            DataFrame con resumen de cambios
        """
        resumen = comparacion.get('resumen', {})

        rows = [
            {'Concepto': 'RESUMEN COMPARACIÓN', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'Mes Anterior', 'Anterior': comparacion.get('mes_anterior', ''),
             'Actual': '-', 'Cambio': ''},
            {'Concepto': 'Mes Actual', 'Anterior': '-',
             'Actual': comparacion.get('mes_actual', ''), 'Cambio': ''},
            {'Concepto': '', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'DOCENTES', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'Total Docentes',
             'Anterior': resumen.get('docentes_anterior', 0),
             'Actual': resumen.get('docentes_actual', 0),
             'Cambio': resumen.get('docentes_actual', 0) - resumen.get('docentes_anterior', 0)},
            {'Concepto': 'Docentes Nuevos',
             'Anterior': '-', 'Actual': resumen.get('docentes_nuevos', 0), 'Cambio': '+'},
            {'Concepto': 'Docentes Salieron',
             'Anterior': resumen.get('docentes_salieron', 0), 'Actual': '-', 'Cambio': '-'},
            {'Concepto': '', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'BRP TOTAL', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'Monto Total',
             'Anterior': f"${resumen.get('brp_anterior', 0):,.0f}",
             'Actual': f"${resumen.get('brp_actual', 0):,.0f}",
             'Cambio': f"{resumen.get('cambio_brp_pct', 0):+.1f}%"},
            {'Concepto': 'BRP SEP',
             'Anterior': f"${resumen.get('sep_anterior', 0):,.0f}",
             'Actual': f"${resumen.get('sep_actual', 0):,.0f}",
             'Cambio': ''},
            {'Concepto': 'BRP PIE',
             'Anterior': f"${resumen.get('pie_anterior', 0):,.0f}",
             'Actual': f"${resumen.get('pie_actual', 0):,.0f}",
             'Cambio': ''},
            {'Concepto': 'BRP NORMAL',
             'Anterior': f"${resumen.get('normal_anterior', 0):,.0f}",
             'Actual': f"${resumen.get('normal_actual', 0):,.0f}",
             'Cambio': ''},
            {'Concepto': '', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'ALERTAS', 'Anterior': '', 'Actual': '', 'Cambio': ''},
            {'Concepto': 'Cambios de Monto >10%',
             'Anterior': '-', 'Actual': resumen.get('cambios_monto_significativo', 0), 'Cambio': ''},
            {'Concepto': 'Cambios de Establecimiento',
             'Anterior': '-', 'Actual': resumen.get('cambios_establecimiento', 0), 'Cambio': ''},
        ]

        return pd.DataFrame(rows)
