"""
Procesador BRP (Bonificación de Reconocimiento Profesional).

Distribuye el BRP según las horas en cada tipo de subvención (SEP, PIE, GENERAL).

Columnas que se dividen:
- Total reconocimiento profesional = Subvención reconocimiento + Transferencia reconocimiento
- Total tramo = Subvención tramo + Transferencia tramo
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

from processors.base import BaseProcessor, ProgressCallback, ProcessorError
from config.columns import WEB_SOSTENEDOR_COLUMNS, normalize_rut


class BRPProcessor(BaseProcessor):
    """Procesador para distribuir BRP entre tipos de subvención."""
    
    MAX_HORAS = 44
    
    def __init__(self):
        super().__init__()
        self.cols = WEB_SOSTENEDOR_COLUMNS
        self.cols_actual = {}
        self.docentes_revisar = []
    
    def process_file(
        self, 
        web_sostenedor_path: Path, 
        sep_procesado_path: Path,
        pie_procesado_path: Path,
        output_path: Path, 
        progress_callback: ProgressCallback
    ) -> None:
        """Procesa y distribuye BRP."""
        try:
            progress_callback(0, "Iniciando distribución BRP...")
            
            # 1. Cargar archivos
            progress_callback(5, "Cargando archivo MINEDUC...")
            df_web = self._load_web_sostenedor(web_sostenedor_path)
            
            progress_callback(15, "Cargando archivo SEP procesado...")
            df_sep = self._load_processed_file(sep_procesado_path, 'SEP')
            
            progress_callback(25, "Cargando archivo PIE procesado...")
            df_pie = self._load_processed_file(pie_procesado_path, 'PIE')
            
            # 2. Construir mapa de horas
            progress_callback(35, "Analizando horas por tipo de subvención...")
            horas_por_docente = self._build_hours_map(df_sep, df_pie)
            
            # 3. Identificar casos para revisión
            progress_callback(40, "Identificando casos para revisión...")
            ruts_web = set(df_web['RUT_NORM'].unique())
            ruts_procesados = set(horas_por_docente.keys())
            self.docentes_revisar = self._build_revision_list(
                horas_por_docente, ruts_web, ruts_procesados, df_web, df_sep, df_pie
            )
            
            # 4. Identificar multi-establecimiento
            progress_callback(50, "Identificando docentes en múltiples establecimientos...")
            df_web = self._identify_multi_establishment(df_web)
            
            # 5. Distribuir BRP por establecimiento
            progress_callback(60, "Distribuyendo BRP por establecimiento...")
            df_web = self._distribute_by_establishment(df_web)
            
            # 6. Clasificar por tipo de subvención
            progress_callback(75, "Clasificando por SEP/PIE/NORMAL...")
            df_result = self._classify_by_subvencion(df_web, horas_por_docente)
            
            # 7. Estadísticas
            progress_callback(85, "Generando resumen...")
            self._log_statistics(df_result)
            
            # 8. Guardar resultado en UN archivo con dos hojas
            progress_callback(90, "Guardando resultados...")
            self._save_combined_file(df_result, output_path)
            
            progress_callback(100, "¡Distribución BRP completada!")
            
        except Exception as e:
            self.logger.error(f"Error en proceso BRP: {str(e)}", exc_info=True)
            raise
    
    def _save_combined_file(self, df_result: pd.DataFrame, output_path: Path) -> None:
        """Guarda resultado y revisión en UN solo archivo con múltiples hojas."""
        with pd.ExcelWriter(str(output_path), engine='openpyxl') as writer:
            
            # Hoja 1: BRP Distribuido (con nombres)
            df_export = self._prepare_export_dataframe(df_result)
            df_export.to_excel(writer, sheet_name='BRP_DISTRIBUIDO', index=False)
            
            # Hoja 2: Resumen por Establecimiento
            df_resumen = self._create_summary_by_rbd(df_result)
            df_resumen.to_excel(writer, sheet_name='RESUMEN_POR_RBD', index=False)
            
            # Hoja 3: Casos a revisar (si hay)
            if self.docentes_revisar:
                df_revision = pd.DataFrame(self.docentes_revisar)
                
                # Ordenar
                df_revision['_orden'] = df_revision['MOTIVO'].map({
                    'EXCEDE 44 HORAS': 0, 
                    'SIN LIQUIDACIÓN': 1
                })
                df_revision = df_revision.sort_values(['_orden', 'HORAS_TOTAL'], ascending=[True, False])
                df_revision = df_revision.drop('_orden', axis=1)
                
                # Reordenar columnas
                cols_order = ['RUT', 'NOMBRE', 'APELLIDOS', 'TIPO_PAGO', 'MOTIVO', 
                              'HORAS_SEP', 'HORAS_PIE', 'HORAS_SN', 'HORAS_TOTAL', 
                              'EXCESO', 'DETALLE', 'ACCION']
                cols_exist = [c for c in cols_order if c in df_revision.columns]
                df_revision = df_revision[cols_exist + [c for c in df_revision.columns if c not in cols_exist]]
                
                df_revision.to_excel(writer, sheet_name='REVISAR', index=False)
                self.logger.info(f"📋 Hoja REVISAR: {len(df_revision)} casos")
            
            # Hoja 4: Resumen General
            df_general = self._create_general_summary(df_result)
            df_general.to_excel(writer, sheet_name='RESUMEN_GENERAL', index=False)
        
        self.logger.info(f"✅ Archivo guardado: {output_path.name}")
    
    def _prepare_export_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepara DataFrame para exportar con nombres y columnas ordenadas."""
        # Columnas a incluir en orden
        col_rbd = self.cols_actual.get('rbd')
        col_rut = self.cols_actual.get('rut')
        col_nombres = self.cols_actual.get('nombres')
        col_ap1 = self.cols_actual.get('apellido1')
        col_ap2 = self.cols_actual.get('apellido2')
        col_horas = self.cols_actual.get('horas_contrato')
        col_tipo_pago = self.cols_actual.get('tipo_pago')
        col_tramo = self.cols_actual.get('tramo')
        
        # Crear columna NOMBRE_COMPLETO
        if col_nombres and col_ap1:
            df['NOMBRE_COMPLETO'] = df.apply(
                lambda r: f"{r.get(col_ap1, '')} {r.get(col_ap2, '')} {r.get(col_nombres, '')}".strip(), 
                axis=1
            )
        
        # Columnas prioritarias al inicio
        cols_inicio = []
        if col_rbd and col_rbd in df.columns:
            cols_inicio.append(col_rbd)
        if col_rut and col_rut in df.columns:
            cols_inicio.append(col_rut)
        if 'NOMBRE_COMPLETO' in df.columns:
            cols_inicio.append('NOMBRE_COMPLETO')
        if col_tipo_pago and col_tipo_pago in df.columns:
            cols_inicio.append(col_tipo_pago)
        if col_tramo and col_tramo in df.columns:
            cols_inicio.append(col_tramo)
        if col_horas and col_horas in df.columns:
            cols_inicio.append(col_horas)
        
        # Columnas BRP
        cols_brp = [
            'BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL',
            'BRP_RECONOCIMIENTO_SEP', 'BRP_RECONOCIMIENTO_PIE', 'BRP_RECONOCIMIENTO_NORMAL',
            'BRP_TRAMO_SEP', 'BRP_TRAMO_PIE', 'BRP_TRAMO_NORMAL'
        ]
        cols_brp = [c for c in cols_brp if c in df.columns]
        
        # Columnas finales (excluyendo las ya agregadas y las internas)
        cols_excluir = set(cols_inicio + cols_brp + ['RUT_NORM', 'PROP_HORAS', 'ES_MULTI', 
                                                      'NUM_ESTABLECIMIENTOS', 'TOTAL_HORAS_MINEDUC',
                                                      'RECONOCIMIENTO_DIST', 'TRAMO_DIST'])
        cols_resto = [c for c in df.columns if c not in cols_excluir]
        
        # Ordenar columnas
        cols_final = cols_inicio + cols_brp + cols_resto
        cols_final = [c for c in cols_final if c in df.columns]
        
        return df[cols_final]
    
    def _create_summary_by_rbd(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea resumen de BRP por establecimiento."""
        col_rbd = self.cols_actual.get('rbd')
        
        if not col_rbd or col_rbd not in df.columns:
            return pd.DataFrame({'Mensaje': ['No se encontró columna RBD']})
        
        # Agrupar por RBD
        resumen = df.groupby(col_rbd).agg({
            'RUT_NORM': 'nunique',
            'BRP_SEP': 'sum',
            'BRP_PIE': 'sum',
            'BRP_NORMAL': 'sum',
            'BRP_TOTAL': 'sum'
        }).reset_index()
        
        resumen.columns = ['RBD', 'DOCENTES', 'BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL']
        
        # Agregar fila de totales
        totales = pd.DataFrame([{
            'RBD': 'TOTAL',
            'DOCENTES': resumen['DOCENTES'].sum(),
            'BRP_SEP': resumen['BRP_SEP'].sum(),
            'BRP_PIE': resumen['BRP_PIE'].sum(),
            'BRP_NORMAL': resumen['BRP_NORMAL'].sum(),
            'BRP_TOTAL': resumen['BRP_TOTAL'].sum()
        }])
        
        resumen = pd.concat([resumen, totales], ignore_index=True)
        
        # Calcular porcentajes
        total_brp = resumen.loc[resumen['RBD'] == 'TOTAL', 'BRP_TOTAL'].values[0]
        if total_brp > 0:
            resumen['%_SEP'] = (resumen['BRP_SEP'] / total_brp * 100).round(1)
            resumen['%_PIE'] = (resumen['BRP_PIE'] / total_brp * 100).round(1)
            resumen['%_NORMAL'] = (resumen['BRP_NORMAL'] / total_brp * 100).round(1)
        
        return resumen
    
    def _create_general_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea resumen general para dashboard."""
        total_docentes = df['RUT_NORM'].nunique()
        col_rbd = self.cols_actual.get('rbd')
        total_rbds = df[col_rbd].nunique() if col_rbd and col_rbd in df.columns else 0
        
        brp_sep = df['BRP_SEP'].sum()
        brp_pie = df['BRP_PIE'].sum()
        brp_normal = df['BRP_NORMAL'].sum()
        brp_total = brp_sep + brp_pie + brp_normal
        
        recon_sep = df['BRP_RECONOCIMIENTO_SEP'].sum()
        recon_pie = df['BRP_RECONOCIMIENTO_PIE'].sum()
        recon_normal = df['BRP_RECONOCIMIENTO_NORMAL'].sum()
        
        tramo_sep = df['BRP_TRAMO_SEP'].sum()
        tramo_pie = df['BRP_TRAMO_PIE'].sum()
        tramo_normal = df['BRP_TRAMO_NORMAL'].sum()
        
        resumen = [
            {'CONCEPTO': 'ESTADÍSTICAS GENERALES', 'VALOR': ''},
            {'CONCEPTO': 'Total Docentes', 'VALOR': total_docentes},
            {'CONCEPTO': 'Total Establecimientos', 'VALOR': total_rbds},
            {'CONCEPTO': 'Casos a Revisar', 'VALOR': len(self.docentes_revisar)},
            {'CONCEPTO': '', 'VALOR': ''},
            {'CONCEPTO': 'DISTRIBUCIÓN BRP', 'VALOR': ''},
            {'CONCEPTO': 'BRP SEP', 'VALOR': f"${brp_sep:,.0f}"},
            {'CONCEPTO': 'BRP PIE', 'VALOR': f"${brp_pie:,.0f}"},
            {'CONCEPTO': 'BRP NORMAL', 'VALOR': f"${brp_normal:,.0f}"},
            {'CONCEPTO': 'BRP TOTAL', 'VALOR': f"${brp_total:,.0f}"},
            {'CONCEPTO': '', 'VALOR': ''},
            {'CONCEPTO': 'PORCENTAJES', 'VALOR': ''},
            {'CONCEPTO': '% SEP', 'VALOR': f"{100*brp_sep/brp_total:.1f}%" if brp_total > 0 else "0%"},
            {'CONCEPTO': '% PIE', 'VALOR': f"{100*brp_pie/brp_total:.1f}%" if brp_total > 0 else "0%"},
            {'CONCEPTO': '% NORMAL', 'VALOR': f"{100*brp_normal/brp_total:.1f}%" if brp_total > 0 else "0%"},
            {'CONCEPTO': '', 'VALOR': ''},
            {'CONCEPTO': 'DETALLE RECONOCIMIENTO', 'VALOR': ''},
            {'CONCEPTO': 'Reconocimiento SEP', 'VALOR': f"${recon_sep:,.0f}"},
            {'CONCEPTO': 'Reconocimiento PIE', 'VALOR': f"${recon_pie:,.0f}"},
            {'CONCEPTO': 'Reconocimiento NORMAL', 'VALOR': f"${recon_normal:,.0f}"},
            {'CONCEPTO': '', 'VALOR': ''},
            {'CONCEPTO': 'DETALLE TRAMO', 'VALOR': ''},
            {'CONCEPTO': 'Tramo SEP', 'VALOR': f"${tramo_sep:,.0f}"},
            {'CONCEPTO': 'Tramo PIE', 'VALOR': f"${tramo_pie:,.0f}"},
            {'CONCEPTO': 'Tramo NORMAL', 'VALOR': f"${tramo_normal:,.0f}"},
        ]
        
        return pd.DataFrame(resumen)
    
    def _load_web_sostenedor(self, path: Path) -> pd.DataFrame:
        """Carga y valida el archivo web_sostenedor."""
        self.validate_file(path)
        
        xlsx = pd.ExcelFile(str(path), engine='openpyxl')
        sheet_name = xlsx.sheet_names[0]
        
        # Intentar leer con header en fila 0, si falla probar fila 1
        df = pd.read_excel(xlsx, sheet_name=sheet_name, header=0)
        
        # Si la primera columna no parece ser RBD, probar con header=1
        if 'Rbd' not in str(df.columns[0]) and 'RBD' not in str(df.columns[0]).upper():
            df = pd.read_excel(xlsx, sheet_name=sheet_name, header=1)
        
        df.columns = df.columns.str.strip()
        
        # Buscar columnas de forma flexible
        def find_col(target):
            target_lower = target.lower().strip()
            for col in df.columns:
                if col.lower().strip() == target_lower:
                    return col
                if target_lower in col.lower():
                    return col
            return None
        
        # Mapear columnas
        self.cols_actual = {
            'rbd': find_col('rbd') or find_col('establecimiento'),
            'rut': find_col('rut (docente)') or find_col('rut'),
            'horas_contrato': find_col('horas de contrato') or find_col('horas'),
            'nombres': find_col('nombres (docente)') or find_col('nombres'),
            'apellido1': find_col('primer apellido') or find_col('apellido'),
            'apellido2': find_col('segundo apellido'),
            'tipo_pago': find_col('tipo de pago'),
            'tramo': find_col('tramo'),
            'total_reconocimiento': find_col('total reconocimiento profesional'),
            'total_tramo': find_col('total tramo'),
            'subv_reconocimiento': find_col('total subvención reconocimiento'),
            'transf_reconocimiento': find_col('total transferencia directa reconocimiento'),
            'subv_tramo': find_col('subvención tramo'),
            'transf_tramo': find_col('transferencia directa tramo'),
        }
        
        # Verificar mínimas requeridas
        required = ['rbd', 'rut', 'horas_contrato']
        missing = [r for r in required if not self.cols_actual.get(r)]
        if missing:
            raise ProcessorError(f"No se encontraron columnas: {missing}")
        
        # Normalizar RUT
        df['RUT_NORM'] = df[self.cols_actual['rut']].apply(normalize_rut)
        
        self.logger.info(f"Columnas mapeadas correctamente")
        return df
    
    def _load_processed_file(self, path: Path, tipo: str) -> pd.DataFrame:
        """Carga archivo procesado (SEP o PIE)."""
        self.validate_file(path)
        
        df = pd.read_excel(str(path), engine='openpyxl')
        
        # Buscar columna RUT
        rut_col = None
        for col in df.columns:
            if 'rut' in str(col).lower():
                rut_col = col
                break
        
        if not rut_col:
            raise ProcessorError(f"Archivo {tipo} no tiene columna de RUT")
        
        df['RUT_NORM'] = df[rut_col].apply(normalize_rut)
        return df
    
    def _build_hours_map(self, df_sep: pd.DataFrame, df_pie: pd.DataFrame) -> Dict:
        """Construye mapa de horas por docente y tipo."""
        horas_map = {}
        
        # Procesar SEP
        for _, row in df_sep.iterrows():
            rut = row.get('RUT_NORM', '')
            if not rut:
                continue
            
            if rut not in horas_map:
                horas_map[rut] = {'SEP': 0, 'PIE': 0, 'SN': 0, 'TOTAL': 0}
            
            sep_hours = float(row.get('SEP', 0) or 0)
            horas_map[rut]['SEP'] += sep_hours
            horas_map[rut]['TOTAL'] += sep_hours
        
        # Procesar PIE
        for _, row in df_pie.iterrows():
            rut = row.get('RUT_NORM', '')
            if not rut:
                continue
            
            if rut not in horas_map:
                horas_map[rut] = {'SEP': 0, 'PIE': 0, 'SN': 0, 'TOTAL': 0}
            
            pie_hours = float(row.get('PIE', 0) or 0)
            sn_hours = float(row.get('SN', 0) or 0)
            
            horas_map[rut]['PIE'] += pie_hours
            horas_map[rut]['SN'] += sn_hours
            horas_map[rut]['TOTAL'] += pie_hours + sn_hours
        
        return horas_map
    
    def _build_revision_list(self, horas_map, ruts_web, ruts_procesados, df_web, df_sep, df_pie) -> List[Dict]:
        """Construye lista de docentes a revisar."""
        revisar = []
        
        # Columnas para obtener info de web_sostenedor
        col_nombres = self.cols_actual.get('nombres')
        col_ap1 = self.cols_actual.get('apellido1')
        col_ap2 = self.cols_actual.get('apellido2')
        col_tipo_pago = self.cols_actual.get('tipo_pago')
        col_horas = self.cols_actual.get('horas_contrato')
        
        def get_docente_info(rut):
            """Obtiene info del docente desde web_sostenedor o archivos procesados."""
            # Primero buscar en web_sostenedor
            doc = df_web[df_web['RUT_NORM'] == rut]
            if len(doc) > 0:
                row = doc.iloc[0]
                nombre = str(row.get(col_nombres, '')) if col_nombres and col_nombres in df_web.columns else ''
                ap1 = str(row.get(col_ap1, '')) if col_ap1 and col_ap1 in df_web.columns else ''
                ap2 = str(row.get(col_ap2, '')) if col_ap2 and col_ap2 in df_web.columns else ''
                apellidos = f"{ap1} {ap2}".strip()
                tipo_pago = str(row.get(col_tipo_pago, '')) if col_tipo_pago and col_tipo_pago in df_web.columns else ''
            else:
                # Buscar en archivos procesados (tienen columna 'nombre' con nombre completo)
                nombre = ''
                apellidos = ''
                tipo_pago = ''
                
                # Buscar en SEP
                doc_sep = df_sep[df_sep['RUT_NORM'] == rut]
                if len(doc_sep) > 0 and 'nombre' in df_sep.columns:
                    nombre_completo = str(doc_sep.iloc[0].get('nombre', ''))
                    if nombre_completo and nombre_completo != 'nan':
                        # El nombre viene como "APELLIDO1 APELLIDO2 NOMBRES"
                        partes = nombre_completo.split()
                        if len(partes) >= 3:
                            apellidos = f"{partes[0]} {partes[1]}"
                            nombre = ' '.join(partes[2:])
                        else:
                            nombre = nombre_completo
                
                # Si no encontró en SEP, buscar en PIE
                if not nombre:
                    doc_pie = df_pie[df_pie['RUT_NORM'] == rut]
                    if len(doc_pie) > 0 and 'nombre' in df_pie.columns:
                        nombre_completo = str(doc_pie.iloc[0].get('nombre', ''))
                        if nombre_completo and nombre_completo != 'nan':
                            partes = nombre_completo.split()
                            if len(partes) >= 3:
                                apellidos = f"{partes[0]} {partes[1]}"
                                nombre = ' '.join(partes[2:])
                            else:
                                nombre = nombre_completo
            
            # Limpiar 'nan'
            nombre = '' if nombre == 'nan' else nombre
            apellidos = '' if apellidos == 'nan' or apellidos == 'nan nan' else apellidos
            tipo_pago = '' if tipo_pago == 'nan' else tipo_pago
            return nombre, apellidos, tipo_pago
        
        # 1. Docentes que exceden 44 horas
        for rut, horas in horas_map.items():
            total = horas['TOTAL']
            if total > self.MAX_HORAS:
                nombre, apellidos, tipo_pago = get_docente_info(rut)
                # Si no hay tipo_pago pero excede horas, probablemente sea reemplazo
                if not tipo_pago and total > self.MAX_HORAS:
                    tipo_pago = '(No en MINEDUC - posible reemplazo)'
                
                revisar.append({
                    'RUT': rut,
                    'NOMBRE': nombre,
                    'APELLIDOS': apellidos,
                    'TIPO_PAGO': tipo_pago,
                    'MOTIVO': 'EXCEDE 44 HORAS',
                    'HORAS_SEP': horas['SEP'],
                    'HORAS_PIE': horas['PIE'],
                    'HORAS_SN': horas['SN'],
                    'HORAS_TOTAL': total,
                    'EXCESO': total - self.MAX_HORAS,
                    'DETALLE': f"SEP:{horas['SEP']:.0f} + PIE:{horas['PIE']:.0f} + SN:{horas['SN']:.0f} = {total:.0f} hrs",
                    'ACCION': 'Verificar si es reemplazante o error'
                })
        
        # 2. Docentes sin liquidación (en MINEDUC pero no en SEP/PIE)
        sin_match = ruts_web - ruts_procesados
        for rut in sin_match:
            doc = df_web[df_web['RUT_NORM'] == rut]
            if len(doc) == 0:
                continue
            
            row = doc.iloc[0]
            nombre = str(row.get(col_nombres, '')) if col_nombres and col_nombres in df_web.columns else ''
            ap1 = str(row.get(col_ap1, '')) if col_ap1 and col_ap1 in df_web.columns else ''
            ap2 = str(row.get(col_ap2, '')) if col_ap2 and col_ap2 in df_web.columns else ''
            apellidos = f"{ap1} {ap2}".strip()
            tipo_pago = str(row.get(col_tipo_pago, '')) if col_tipo_pago and col_tipo_pago in df_web.columns else ''
            horas_contrato = row.get(col_horas, 0) if col_horas and col_horas in df_web.columns else 0
            
            # Limpiar 'nan'
            nombre = '' if nombre == 'nan' else nombre
            apellidos = '' if apellidos == 'nan' or apellidos == 'nan nan' else apellidos
            tipo_pago = '' if tipo_pago == 'nan' else tipo_pago
            
            revisar.append({
                'RUT': rut,
                'NOMBRE': nombre,
                'APELLIDOS': apellidos,
                'TIPO_PAGO': tipo_pago,
                'MOTIVO': 'SIN LIQUIDACIÓN',
                'HORAS_SEP': 0,
                'HORAS_PIE': 0,
                'HORAS_SN': 0,
                'HORAS_TOTAL': 0,
                'HORAS_CONTRATO_MINEDUC': horas_contrato,
                'DETALLE': 'En MINEDUC pero no en archivos SEP/PIE',
                'ACCION': 'Verificar licencia, nuevo ingreso, o falta en liquidaciones'
            })
        
        # Log
        exceden = len([r for r in revisar if r['MOTIVO'] == 'EXCEDE 44 HORAS'])
        sin_liq = len([r for r in revisar if r['MOTIVO'] == 'SIN LIQUIDACIÓN'])
        reemplazos = len([r for r in revisar if 'reemplazo' in r.get('TIPO_PAGO', '').lower()])
        
        if exceden > 0:
            self.logger.warning(f"⚠️ {exceden} docentes exceden 44 horas")
        if reemplazos > 0:
            self.logger.info(f"ℹ️ {reemplazos} de ellos son tipo 'Reemplazo'")
        if sin_liq > 0:
            self.logger.warning(f"⚠️ {sin_liq} docentes sin liquidación SEP/PIE")
        
        return revisar
    
    def _save_revision_file(self, path: Path) -> None:
        """Guarda archivo de revisión."""
        df = pd.DataFrame(self.docentes_revisar)
        
        # Ordenar
        df['_orden'] = df['MOTIVO'].map({'EXCEDE 44 HORAS': 0, 'SIN LIQUIDACIÓN': 1})
        df = df.sort_values(['_orden', 'HORAS_TOTAL'], ascending=[True, False])
        df = df.drop('_orden', axis=1)
        
        # Reordenar columnas
        cols_order = ['RUT', 'NOMBRE', 'APELLIDOS', 'TIPO_PAGO', 'MOTIVO', 
                      'HORAS_SEP', 'HORAS_PIE', 'HORAS_SN', 'HORAS_TOTAL', 
                      'EXCESO', 'DETALLE', 'ACCION']
        cols_exist = [c for c in cols_order if c in df.columns]
        df = df[cols_exist + [c for c in df.columns if c not in cols_exist]]
        
        self.safe_save(df, path)
        self.logger.info(f"📋 Archivo de revisión: {path.name} ({len(df)} casos)")
    
    def _identify_multi_establishment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identifica docentes en múltiples establecimientos."""
        col_horas = self.cols_actual['horas_contrato']
        col_rbd = self.cols_actual['rbd']
        
        stats = df.groupby('RUT_NORM').agg({
            col_rbd: 'nunique',
            col_horas: 'sum'
        }).reset_index()
        stats.columns = ['RUT_NORM', 'NUM_ESTABLECIMIENTOS', 'TOTAL_HORAS_MINEDUC']
        
        df = df.merge(stats, on='RUT_NORM', how='left')
        df['ES_MULTI'] = df['NUM_ESTABLECIMIENTOS'] > 1
        
        return df
    
    def _distribute_by_establishment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Distribuye montos proporcionalmente por establecimiento."""
        col_horas = self.cols_actual['horas_contrato']
        
        # Proporción de horas
        df['PROP_HORAS'] = df[col_horas] / df['TOTAL_HORAS_MINEDUC']
        df['PROP_HORAS'] = df['PROP_HORAS'].replace([np.inf, -np.inf, np.nan], 0)
        
        # Distribuir montos por establecimiento
        col_total_recon = self.cols_actual.get('total_reconocimiento')
        col_total_tramo = self.cols_actual.get('total_tramo')
        
        if col_total_recon and col_total_recon in df.columns:
            df['RECONOCIMIENTO_DIST'] = (df[col_total_recon] * df['PROP_HORAS']).round(0)
        else:
            df['RECONOCIMIENTO_DIST'] = 0
        
        if col_total_tramo and col_total_tramo in df.columns:
            df['TRAMO_DIST'] = (df[col_total_tramo] * df['PROP_HORAS']).round(0)
        else:
            df['TRAMO_DIST'] = 0
        
        return df
    
    def _classify_by_subvencion(self, df: pd.DataFrame, horas_map: Dict) -> pd.DataFrame:
        """Clasifica BRP por tipo de subvención (SEP/PIE/NORMAL)."""
        
        # Inicializar columnas - usar NORMAL en vez de GENERAL
        df['BRP_RECONOCIMIENTO_SEP'] = 0.0
        df['BRP_RECONOCIMIENTO_PIE'] = 0.0
        df['BRP_RECONOCIMIENTO_NORMAL'] = 0.0
        df['BRP_TRAMO_SEP'] = 0.0
        df['BRP_TRAMO_PIE'] = 0.0
        df['BRP_TRAMO_NORMAL'] = 0.0
        
        for idx, row in df.iterrows():
            rut = row.get('RUT_NORM', '')
            recon_dist = row.get('RECONOCIMIENTO_DIST', 0) or 0
            tramo_dist = row.get('TRAMO_DIST', 0) or 0
            
            if not rut:
                continue
            
            # Obtener proporción de horas
            horas = horas_map.get(rut, {'SEP': 0, 'PIE': 0, 'SN': 0, 'TOTAL': 0})
            total_horas = horas['TOTAL']
            
            if total_horas == 0:
                # Sin info de horas, todo va a NORMAL
                df.at[idx, 'BRP_RECONOCIMIENTO_NORMAL'] = recon_dist
                df.at[idx, 'BRP_TRAMO_NORMAL'] = tramo_dist
                continue
            
            # Calcular proporciones
            prop_sep = horas['SEP'] / total_horas
            prop_pie = horas['PIE'] / total_horas
            prop_sn = horas['SN'] / total_horas  # SN = Subvención Normal
            
            # Distribuir Reconocimiento
            df.at[idx, 'BRP_RECONOCIMIENTO_SEP'] = round(recon_dist * prop_sep)
            df.at[idx, 'BRP_RECONOCIMIENTO_PIE'] = round(recon_dist * prop_pie)
            df.at[idx, 'BRP_RECONOCIMIENTO_NORMAL'] = round(recon_dist * prop_sn)
            
            # Distribuir Tramo
            df.at[idx, 'BRP_TRAMO_SEP'] = round(tramo_dist * prop_sep)
            df.at[idx, 'BRP_TRAMO_PIE'] = round(tramo_dist * prop_pie)
            df.at[idx, 'BRP_TRAMO_NORMAL'] = round(tramo_dist * prop_sn)
        
        # Totales por tipo
        df['BRP_SEP'] = df['BRP_RECONOCIMIENTO_SEP'] + df['BRP_TRAMO_SEP']
        df['BRP_PIE'] = df['BRP_RECONOCIMIENTO_PIE'] + df['BRP_TRAMO_PIE']
        df['BRP_NORMAL'] = df['BRP_RECONOCIMIENTO_NORMAL'] + df['BRP_TRAMO_NORMAL']
        df['BRP_TOTAL'] = df['BRP_SEP'] + df['BRP_PIE'] + df['BRP_NORMAL']
        
        # Convertir a enteros
        for col in ['BRP_RECONOCIMIENTO_SEP', 'BRP_RECONOCIMIENTO_PIE', 'BRP_RECONOCIMIENTO_NORMAL',
                    'BRP_TRAMO_SEP', 'BRP_TRAMO_PIE', 'BRP_TRAMO_NORMAL',
                    'BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL']:
            df[col] = df[col].fillna(0).astype(int)
        
        return df
    
    def _log_statistics(self, df: pd.DataFrame) -> None:
        """Genera estadísticas."""
        total_docentes = df['RUT_NORM'].nunique()
        
        brp_sep = df['BRP_SEP'].sum()
        brp_pie = df['BRP_PIE'].sum()
        brp_normal = df['BRP_NORMAL'].sum()
        brp_total = brp_sep + brp_pie + brp_normal
        
        recon_total = df['RECONOCIMIENTO_DIST'].sum() if 'RECONOCIMIENTO_DIST' in df.columns else 0
        tramo_total = df['TRAMO_DIST'].sum() if 'TRAMO_DIST' in df.columns else 0
        
        self.logger.info("=" * 60)
        self.logger.info("RESUMEN DE DISTRIBUCIÓN BRP")
        self.logger.info("=" * 60)
        self.logger.info(f"Total docentes: {total_docentes}")
        self.logger.info("-" * 60)
        self.logger.info(f"Reconocimiento Profesional: ${recon_total:,.0f}")
        self.logger.info(f"Tramo:                      ${tramo_total:,.0f}")
        self.logger.info("-" * 60)
        if brp_total > 0:
            self.logger.info(f"→ SEP:    ${brp_sep:,.0f} ({100*brp_sep/brp_total:.1f}%)")
            self.logger.info(f"→ PIE:    ${brp_pie:,.0f} ({100*brp_pie/brp_total:.1f}%)")
            self.logger.info(f"→ NORMAL: ${brp_normal:,.0f} ({100*brp_normal/brp_total:.1f}%)")
        self.logger.info("=" * 60)
