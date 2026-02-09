"""
Procesador para remuneraciones EIB (Educación Intercultural Bilingüe).

Los docentes EIB tienen BRP=$0 (no aparecen en web sostenedor).
El archivo EIB tiene 'Hoja1' con datos salariales y columna 'Jornada' con horas.
Es 100% EIB: ratio=1.0, no requiere prorrateo entre subvenciones.
"""

from pathlib import Path
import pandas as pd

from processors.base import BaseProcessor, ProgressCallback
from config.columns import (
    SALARY_BENEFIT_COLUMNS,
    SPECIAL_SALARY_COLUMNS,
)


class EIBProcessor(BaseProcessor):
    """
    Procesador especializado para remuneraciones EIB.

    A diferencia de SEP/PIE, EIB no tiene hojas HORAS+TOTAL separadas.
    Usa una sola hoja ('Hoja1') con columna 'Jornada' para las horas.
    Todo es 100% EIB (ratio=1.0).
    """

    def process_file(
        self,
        input_path: Path,
        output_path: Path,
        progress_callback: ProgressCallback,
    ) -> None:
        """Procesa archivo de remuneraciones EIB."""
        try:
            progress_callback(0, "Iniciando proceso EIB...")

            # Cargar datos
            progress_callback(5, "Cargando datos...")
            self.validate_file(input_path)
            df = self._load_eib_sheet(input_path)

            progress_callback(20, "Normalizando datos...")

            # Normalizar columna Rut (EIB puede usar 'rut' minúscula)
            if 'rut' in df.columns and 'Rut' not in df.columns:
                df = df.rename(columns={'rut': 'Rut'})

            # Validar columna de horas
            hours_col = self.config.EIB_HOURS_COL
            if hours_col not in df.columns:
                raise ValueError(
                    f"No se encontró la columna '{hours_col}' en el archivo EIB. "
                    f"Columnas disponibles: {list(df.columns)}"
                )

            # Asegurar valores numéricos en columna de horas
            df[hours_col] = pd.to_numeric(df[hours_col], errors='coerce').fillna(0)

            # 100% EIB: total horas = jornada (ratio=1.0)
            df['TOTAL HORAS POR DOCENTE'] = df[hours_col]

            progress_callback(40, "Calculando salarios proporcionales...")

            # Prorratear columnas con sufijo _EIB
            all_salary_columns = SPECIAL_SALARY_COLUMNS + SALARY_BENEFIT_COLUMNS
            df = self.prorate_columns(
                df,
                columns=all_salary_columns,
                hours_column=hours_col,
                total_hours_column='TOTAL HORAS POR DOCENTE',
                output_suffix='_EIB',
            )

            progress_callback(70, "Validando horas...")
            df = self.validate_hours(df)

            # Ordenar
            if 'Rut' in df.columns and 'Nombre' in df.columns:
                df = df.sort_values(['Rut', 'Nombre'])
            elif 'Rut' in df.columns:
                df = df.sort_values('Rut')

            progress_callback(90, "Guardando resultados...")
            self.safe_save(df, output_path)

            progress_callback(100, "Proceso EIB completado!")

        except Exception as e:
            self.logger.error(f"Error en proceso EIB: {str(e)}", exc_info=True)
            raise

    def _load_eib_sheet(self, file_path: Path) -> pd.DataFrame:
        """Carga la hoja del archivo EIB (CSV o Excel)."""
        if self.is_csv(file_path):
            return self.load_datafile(file_path)
        try:
            return self.load_excel_with_retry(file_path, 'Hoja1')
        except (ValueError, KeyError):
            self.logger.info("Hoja 'Hoja1' no encontrada, usando primera hoja")
            with pd.ExcelFile(str(file_path), engine='openpyxl') as xlsx:
                if not xlsx.sheet_names:
                    raise ValueError("El archivo no contiene hojas")
                first_sheet = xlsx.sheet_names[0]
            return self.load_excel_with_retry(file_path, first_sheet)
