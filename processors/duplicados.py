"""
Procesador para consolidar registros duplicados.
"""

import logging
from pathlib import Path
from typing import List
import pandas as pd

from processors.base import BaseProcessor, ProgressCallback, ProcessorError


class DuplicadosProcessor(BaseProcessor):
    """
    Procesador para consolidar registros duplicados.
    
    Identifica registros duplicados basados en una columna clave,
    suma los valores numéricos de las columnas especificadas, y
    elimina las filas duplicadas manteniendo solo la primera.
    """
    
    def __init__(self, duplicate_column: str = 'DUPLICADOS'):
        super().__init__()
        self.duplicate_column = duplicate_column
    
    def process_file(
        self,
        input_path1: Path,
        input_path2: Path,
        output_path: Path,
        progress_callback: ProgressCallback
    ) -> None:
        """
        Procesa duplicados entre dos archivos.
        
        Args:
            input_path1: Archivo principal con datos
            input_path2: Archivo complementario (usado para validación/cruce)
            output_path: Donde guardar el resultado consolidado
            progress_callback: Función para reportar progreso
        """
        try:
            progress_callback(0, "Iniciando proceso de duplicados...")
            
            # Cargar archivos
            progress_callback(10, "Cargando primer archivo...")
            self.validate_file(input_path1)
            df = self._load_excel_safe(input_path1)
            
            progress_callback(20, "Cargando segundo archivo...")
            self.validate_file(input_path2)
            df_extra = self._load_excel_safe(input_path2)
            
            progress_callback(30, "Detectando duplicados...")
            
            # Validar columna de duplicados
            if self.duplicate_column not in df.columns:
                raise ProcessorError(
                    f"La columna '{self.duplicate_column}' no existe en el archivo. "
                    "Verifique la estructura del archivo."
                )
            
            # Procesar duplicados
            df = self._process_duplicates(df, progress_callback)
            
            progress_callback(80, "Guardando resultado final...")
            self.safe_save(df, output_path)
            
            progress_callback(100, f"¡Proceso completado! Archivo guardado en {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error en DuplicadosProcessor: {str(e)}", exc_info=True)
            raise
    
    def _load_excel_safe(self, path: Path) -> pd.DataFrame:
        """Carga Excel con manejo de errores específico."""
        return self.load_excel_with_retry(path, sheet_name='Hoja1')
    
    def _process_duplicates(
        self, 
        df: pd.DataFrame, 
        progress_callback: ProgressCallback
    ) -> pd.DataFrame:
        """
        Procesa y consolida registros duplicados.
        """
        dup_col = self.duplicate_column
        
        # Identificar duplicados
        duplicados_mask = df.duplicated(subset=[dup_col], keep=False)
        df_duplicados = df[duplicados_mask]
        
        if df_duplicados.empty:
            self.logger.info("No se encontraron registros duplicados")
            progress_callback(50, "No se encontraron duplicados")
            return df.sort_values(by=dup_col)
        
        num_duplicados = len(df_duplicados)
        self.logger.info(f"Se encontraron {num_duplicados} registros duplicados")
        progress_callback(40, f"Procesando {num_duplicados} duplicados...")
        
        # Determinar columnas a sumar (desde columna 17 o todas numéricas)
        columnas_suma = self._get_sum_columns(df)
        
        # Agrupar y sumar
        progress_callback(50, "Calculando sumas...")
        try:
            df_suma = df_duplicados.groupby(dup_col)[columnas_suma].sum().reset_index()
        except Exception as e:
            self.logger.error(f"Error al agrupar duplicados: {str(e)}")
            raise ProcessorError(f"Error al procesar duplicados: {str(e)}")
        
        # Actualizar valores
        progress_callback(60, "Actualizando registros...")
        for _, row in df_suma.iterrows():
            mask = df[dup_col] == row[dup_col]
            df.loc[mask, columnas_suma] = row[columnas_suma].values
        
        # Eliminar duplicados
        progress_callback(70, "Eliminando duplicados adicionales...")
        num_antes = len(df)
        df = df.drop_duplicates(subset=[dup_col], keep='first')
        num_despues = len(df)
        
        eliminados = num_antes - num_despues
        self.logger.info(f"Se eliminaron {eliminados} filas duplicadas")
        
        # Ordenar
        return df.sort_values(by=dup_col)
    
    def _get_sum_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Determina las columnas a sumar.
        
        Por defecto usa columnas desde la posición 16 (índice 0).
        Si hay menos columnas, usa todas las numéricas.
        """
        if len(df.columns) >= 17:
            columnas_suma = list(df.columns[16:])
        else:
            self.logger.warning(
                f"El archivo tiene {len(df.columns)} columnas (menos de 17). "
                "Se usarán todas las columnas numéricas."
            )
            columnas_suma = df.select_dtypes(include=['number']).columns.tolist()
            
            # Remover columna de duplicados si está
            if self.duplicate_column in columnas_suma:
                columnas_suma.remove(self.duplicate_column)
        
        return columnas_suma
