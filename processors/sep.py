"""
Procesador para remuneraciones SEP (Subvención Escolar Preferencial).
"""

from pathlib import Path
import pandas as pd

from processors.base import BaseProcessor, ProgressCallback
from config.columns import (
    SALARY_BENEFIT_COLUMNS,
    SPECIAL_SALARY_COLUMNS,
    get_available_columns
)


class SEPProcessor(BaseProcessor):
    """
    Procesador especializado para remuneraciones SEP.
    
    Calcula el prorrateo de salarios y beneficios según las horas SEP
    asignadas a cada docente por establecimiento.
    """
    
    def process_file(
        self, 
        input_path: Path, 
        output_path: Path, 
        progress_callback: ProgressCallback
    ) -> None:
        """Procesa archivo de remuneraciones para SEP."""
        try:
            progress_callback(0, "Iniciando proceso SEP...")
            
            # Cargar datos
            progress_callback(5, "Cargando datos...")
            df_horas, df_total = self.load_sheets(input_path)
            
            # Validar columnas requeridas
            self.validate_columns(
                df_horas, 
                self.config.REQUIRED_HORAS | {self.config.SEP_HOURS_COL},
                'HORAS'
            )
            self.validate_columns(df_total, self.config.REQUIRED_TOTAL, 'TOTAL')
            
            progress_callback(20, "Calculando horas por docente...")
            
            # Procesar datos
            result = self._process_data(df_horas, df_total, progress_callback)
            
            # Guardar
            progress_callback(90, "Guardando resultados...")
            self.safe_save(result, output_path)
            
            progress_callback(100, "¡Proceso SEP completado!")
            
        except Exception as e:
            self.logger.error(f"Error en proceso SEP: {str(e)}", exc_info=True)
            raise
    
    def _process_data(
        self, 
        df_horas: pd.DataFrame, 
        df_total: pd.DataFrame,
        progress_callback: ProgressCallback
    ) -> pd.DataFrame:
        """Lógica principal de procesamiento."""
        
        # Agregar IDs para tracking
        df_horas['ID_Horas'] = df_horas.index
        df_total['ID_Total'] = df_total.index
        
        # Calcular total de horas por docente
        df_horas = self.calculate_total_hours_by_teacher(
            df_horas,
            hours_columns=[self.config.SEP_HOURS_COL]
        )
        
        progress_callback(30, "Combinando datos...")
        
        # Combinar con datos de total
        datos = pd.merge(df_total, df_horas, on=['Rut'], how='left').reset_index(drop=True)
        
        # Rellenar valores faltantes
        datos = datos.fillna({
            self.config.SEP_HOURS_COL: 0,
            'TOTAL HORAS POR DOCENTE': 0
        })
        
        progress_callback(50, "Calculando salarios proporcionales...")
        
        # Prorratear columnas de salario
        all_salary_columns = SPECIAL_SALARY_COLUMNS + SALARY_BENEFIT_COLUMNS
        datos = self.prorate_columns(
            datos,
            columns=all_salary_columns,
            hours_column=self.config.SEP_HOURS_COL,
            total_hours_column='TOTAL HORAS POR DOCENTE',
            output_suffix='_SEP'
        )
        
        progress_callback(70, "Validando horas...")
        
        # Validar horas
        datos = self.validate_hours(datos)
        
        # Limpiar columnas auxiliares
        for col in ['ID_Horas', 'ID_Total']:
            if col in datos.columns:
                datos = datos.drop(col, axis=1)
        
        # Ordenar por Rut y Nombre
        if 'Nombre' in datos.columns:
            datos = datos.sort_values(['Rut', 'Nombre'])
        else:
            datos = datos.sort_values('Rut')
        
        return datos
