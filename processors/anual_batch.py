"""
Procesador de Lote Anual - Procesa 12 meses de SEP+PIE+BRP(+EIB) de golpe.

Recibe ~48 archivos (4 por mes: web, sep, pie, eib opcional),
los clasifica por mes y tipo, y genera un Excel consolidado anual.
"""

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from config.columns import (
    detect_month_from_filename,
    detect_file_type,
    MESES_NUM_TO_NAME,
)
from processors.sep import SEPProcessor
from processors.pie import PIEProcessor
from processors.eib import EIBProcessor
from processors.brp import BRPProcessor


@dataclass
class MonthlyFileSet:
    """Conjunto de archivos para un mes."""
    month: str  # '01'-'12'
    month_name: str
    sep: Optional[Tuple[str, Path]] = None   # (filename, path)
    pie: Optional[Tuple[str, Path]] = None
    eib: Optional[Tuple[str, Path]] = None
    web: Optional[Tuple[str, Path]] = None


class AnualBatchProcessor:
    """Procesador de lote anual: 12 meses de SEP+PIE+BRP+EIB."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def classify_files(
        self, files: List[Tuple[str, Path]]
    ) -> Dict[str, MonthlyFileSet]:
        """
        Clasifica archivos por mes y tipo.

        Args:
            files: Lista de (filename, path) tuplas.

        Returns:
            Dict con keys '01'-'12', valores MonthlyFileSet.
        """
        monthly: Dict[str, MonthlyFileSet] = {}

        for filename, path in files:
            month = detect_month_from_filename(filename)
            ftype = detect_file_type(filename)

            if not month or not ftype:
                self.logger.warning(
                    f"No se pudo clasificar: {filename} "
                    f"(mes={month}, tipo={ftype})"
                )
                continue

            if month not in monthly:
                month_name = MESES_NUM_TO_NAME.get(month, month)
                monthly[month] = MonthlyFileSet(
                    month=month, month_name=month_name
                )

            ms = monthly[month]
            entry = (filename, path)
            if ftype == 'sep':
                ms.sep = entry
            elif ftype == 'pie':
                ms.pie = entry
            elif ftype == 'eib':
                ms.eib = entry
            elif ftype == 'web':
                ms.web = entry

        return monthly

    def validate_monthly_sets(
        self, monthly: Dict[str, MonthlyFileSet]
    ) -> List[str]:
        """
        Valida que cada mes tenga los archivos requeridos (SEP, PIE, WEB).

        Returns:
            Lista de errores. Vacía si todo está OK.
        """
        errors = []
        for month_num in sorted(monthly.keys()):
            ms = monthly[month_num]
            missing = []
            if not ms.sep:
                missing.append('SEP')
            if not ms.pie:
                missing.append('PIE')
            if not ms.web:
                missing.append('WEB')
            if missing:
                errors.append(
                    f"{ms.month_name} ({month_num}): faltan {', '.join(missing)}"
                )
        return errors

    def process_all(
        self,
        monthly_sets: Dict[str, MonthlyFileSet],
        output_path: Path,
        progress_callback: Callable[[int, str], None],
    ) -> Dict:
        """
        Procesa todos los meses y genera Excel consolidado.

        Returns:
            Dict con estadísticas del procesamiento.
        """
        if not monthly_sets:
            raise ValueError("No hay meses para procesar")

        all_brp = []
        all_eib = []
        month_summaries = []
        total_months = len(monthly_sets)
        processed = 0

        def noop_progress(val, msg):
            pass

        for month_num in sorted(monthly_sets.keys()):
            ms = monthly_sets[month_num]
            pct_base = int((processed / total_months) * 90)
            progress_callback(
                pct_base, f"Procesando {ms.month_name}..."
            )

            tmp_files = []
            try:
                # 1. Procesar SEP
                sep_out = self._make_temp()
                tmp_files.append(sep_out)
                sep_proc = SEPProcessor()
                sep_proc.process_file(ms.sep[1], sep_out, noop_progress)

                # 2. Procesar PIE
                pie_out = self._make_temp()
                tmp_files.append(pie_out)
                pie_proc = PIEProcessor()
                pie_proc.process_file(ms.pie[1], pie_out, noop_progress)

                # 3. Procesar EIB (opcional)
                eib_df = None
                if ms.eib:
                    eib_out = self._make_temp()
                    tmp_files.append(eib_out)
                    eib_proc = EIBProcessor()
                    eib_proc.process_file(ms.eib[1], eib_out, noop_progress)
                    eib_df = pd.read_excel(eib_out, engine='openpyxl')
                    eib_df['MES'] = ms.month_name
                    eib_df['MES_NUM'] = month_num
                    all_eib.append(eib_df)

                # 4. Procesar BRP
                brp_out = self._make_temp()
                tmp_files.append(brp_out)
                brp_proc = BRPProcessor()
                brp_proc.process_file(
                    web_sostenedor_path=ms.web[1],
                    sep_procesado_path=sep_out,
                    pie_procesado_path=pie_out,
                    output_path=brp_out,
                    progress_callback=noop_progress,
                )

                # 5. Leer BRP_DISTRIBUIDO
                brp_df = pd.read_excel(
                    brp_out, sheet_name='BRP_DISTRIBUIDO', engine='openpyxl'
                )
                brp_df['MES'] = ms.month_name
                brp_df['MES_NUM'] = month_num
                all_brp.append(brp_df)

                # 6. Resumen del mes
                summary = {
                    'MES': ms.month_name,
                    'MES_NUM': month_num,
                    'BRP_SEP': brp_df['BRP_SEP'].sum() if 'BRP_SEP' in brp_df.columns else 0,
                    'BRP_PIE': brp_df['BRP_PIE'].sum() if 'BRP_PIE' in brp_df.columns else 0,
                    'BRP_NORMAL': brp_df['BRP_NORMAL'].sum() if 'BRP_NORMAL' in brp_df.columns else 0,
                    'BRP_TOTAL': brp_df['BRP_TOTAL'].sum() if 'BRP_TOTAL' in brp_df.columns else 0,
                    'DOCENTES_BRP': brp_df['RUT (Docente)'].nunique() if 'RUT (Docente)' in brp_df.columns else len(brp_df),
                    'COSTO_EIB': int(eib_df['TOTAL HABERES_EIB'].sum()) if eib_df is not None and 'TOTAL HABERES_EIB' in eib_df.columns else 0,
                    'DOCENTES_EIB': len(eib_df) if eib_df is not None else 0,
                    'CON_EIB': ms.eib is not None,
                }
                month_summaries.append(summary)

            except Exception as e:
                self.logger.error(
                    f"Error procesando {ms.month_name}: {e}", exc_info=True
                )
                month_summaries.append({
                    'MES': ms.month_name,
                    'MES_NUM': month_num,
                    'BRP_SEP': 0, 'BRP_PIE': 0, 'BRP_NORMAL': 0,
                    'BRP_TOTAL': 0, 'DOCENTES_BRP': 0,
                    'COSTO_EIB': 0, 'DOCENTES_EIB': 0,
                    'CON_EIB': False,
                    'ERROR': str(e),
                })
            finally:
                for tf in tmp_files:
                    self._cleanup(tf)

            processed += 1

        progress_callback(92, "Generando resumen anual...")

        # Generar Excel consolidado
        self._write_output(
            output_path, all_brp, all_eib, month_summaries
        )

        progress_callback(100, "Lote anual completado!")

        # Estadísticas de retorno
        df_summary = pd.DataFrame(month_summaries)
        return {
            'meses_procesados': len([s for s in month_summaries if 'ERROR' not in s]),
            'meses_error': len([s for s in month_summaries if 'ERROR' in s]),
            'brp_total_anual': int(df_summary['BRP_TOTAL'].sum()),
            'eib_total_anual': int(df_summary['COSTO_EIB'].sum()),
            'summaries': month_summaries,
        }

    def _write_output(
        self,
        output_path: Path,
        all_brp: List[pd.DataFrame],
        all_eib: List[pd.DataFrame],
        month_summaries: List[Dict],
    ) -> None:
        """Escribe Excel multi-hoja con resultados anuales."""
        df_summary = pd.DataFrame(month_summaries)

        with pd.ExcelWriter(str(output_path), engine='openpyxl') as writer:
            # Hoja 1: RESUMEN_ANUAL
            resumen_rows = []
            brp_sep = int(df_summary['BRP_SEP'].sum())
            brp_pie = int(df_summary['BRP_PIE'].sum())
            brp_normal = int(df_summary['BRP_NORMAL'].sum())
            brp_total = brp_sep + brp_pie + brp_normal
            eib_total = int(df_summary['COSTO_EIB'].sum())

            resumen_rows.append({'CONCEPTO': 'BRP SEP', 'MONTO': brp_sep})
            resumen_rows.append({'CONCEPTO': 'BRP PIE', 'MONTO': brp_pie})
            resumen_rows.append({'CONCEPTO': 'BRP NORMAL', 'MONTO': brp_normal})
            resumen_rows.append({'CONCEPTO': 'BRP TOTAL', 'MONTO': brp_total})
            resumen_rows.append({'CONCEPTO': 'COSTO EIB TOTAL', 'MONTO': eib_total})
            resumen_rows.append({'CONCEPTO': 'GRAN TOTAL', 'MONTO': brp_total + eib_total})
            pd.DataFrame(resumen_rows).to_excel(
                writer, sheet_name='RESUMEN_ANUAL', index=False
            )

            # Hoja 2: POR_MES
            cols_mes = [
                'MES', 'BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL',
                'COSTO_EIB', 'DOCENTES_BRP', 'DOCENTES_EIB',
            ]
            cols_exist = [c for c in cols_mes if c in df_summary.columns]
            df_por_mes = df_summary[cols_exist].copy()
            # Agregar fila de totales
            totals = {c: df_por_mes[c].sum() for c in cols_exist if c != 'MES'}
            totals['MES'] = 'TOTAL'
            df_por_mes = pd.concat(
                [df_por_mes, pd.DataFrame([totals])], ignore_index=True
            )
            df_por_mes.to_excel(writer, sheet_name='POR_MES', index=False)

            # Hoja 3: POR_RBD (totales anuales por establecimiento)
            if all_brp:
                df_all_brp = pd.concat(all_brp, ignore_index=True)
                rbd_col = None
                for c in df_all_brp.columns:
                    if 'rbd' in c.lower():
                        rbd_col = c
                        break
                if rbd_col:
                    brp_agg_cols = {}
                    for c in ['BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL']:
                        if c in df_all_brp.columns:
                            brp_agg_cols[c] = 'sum'
                    if brp_agg_cols:
                        df_rbd = df_all_brp.groupby(rbd_col).agg(brp_agg_cols).reset_index()
                        df_rbd = df_rbd.rename(columns={rbd_col: 'RBD'})
                        df_rbd.to_excel(writer, sheet_name='POR_RBD', index=False)

                # Hoja 4: DETALLE_BRP
                df_all_brp.to_excel(
                    writer, sheet_name='DETALLE_BRP', index=False
                )

            # Hoja 5: DETALLE_EIB
            if all_eib:
                df_all_eib = pd.concat(all_eib, ignore_index=True)
                df_all_eib.to_excel(
                    writer, sheet_name='DETALLE_EIB', index=False
                )

    def _make_temp(self) -> Path:
        """Crea un archivo temporal."""
        tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        tmp.close()
        return Path(tmp.name)

    def _cleanup(self, path: Path) -> None:
        """Elimina archivo temporal."""
        try:
            if path and path.exists():
                os.unlink(path)
        except OSError:
            pass
