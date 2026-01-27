"""
Repositorio para operaciones CRUD sobre la base de datos de BRP.
"""

import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

from database.models import Base, ProcesamientoMensual, DocenteMensual


class BRPRepository:
    """
    Repositorio para gestionar el almacenamiento histórico de BRP.

    Permite guardar procesamiento, consultar meses disponibles
    y obtener datos para comparaciones.
    """

    def __init__(self, db_path: str = "data/remupro.db"):
        """
        Inicializa el repositorio.

        Args:
            db_path: Ruta al archivo SQLite (relativa o absoluta)
        """
        self.db_path = Path(db_path)
        self._ensure_data_dir()

        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=False,
            connect_args={"check_same_thread": False}
        )
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Crear tablas si no existen
        Base.metadata.create_all(self.engine)

    def _ensure_data_dir(self) -> None:
        """Crea el directorio data si no existe."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_session(self) -> Session:
        """Obtiene una sesión de base de datos."""
        return self.SessionLocal()

    def guardar_procesamiento(
        self,
        mes: str,
        df: pd.DataFrame,
        notas: str = ""
    ) -> ProcesamientoMensual:
        """
        Guarda un procesamiento mensual completo.

        Args:
            mes: Identificador del mes (ej: "2024-01")
            df: DataFrame con los resultados del procesamiento
            notas: Notas opcionales

        Returns:
            El objeto ProcesamientoMensual creado
        """
        session = self._get_session()

        try:
            # Eliminar procesamiento anterior del mismo mes si existe
            anterior = session.query(ProcesamientoMensual).filter_by(mes=mes).first()
            if anterior:
                session.delete(anterior)
                session.commit()

            # Calcular estadísticas
            brp_sep = df['BRP_SEP'].sum() if 'BRP_SEP' in df.columns else 0
            brp_pie = df['BRP_PIE'].sum() if 'BRP_PIE' in df.columns else 0
            brp_normal = df['BRP_NORMAL'].sum() if 'BRP_NORMAL' in df.columns else 0
            brp_total = brp_sep + brp_pie + brp_normal

            # Identificar columnas de reconocimiento y tramo
            recon_cols = ['BRP_RECONOCIMIENTO_SEP', 'BRP_RECONOCIMIENTO_PIE', 'BRP_RECONOCIMIENTO_NORMAL']
            tramo_cols = ['BRP_TRAMO_SEP', 'BRP_TRAMO_PIE', 'BRP_TRAMO_NORMAL']

            reconocimiento_total = sum(
                df[col].sum() for col in recon_cols if col in df.columns
            )
            tramo_total = sum(
                df[col].sum() for col in tramo_cols if col in df.columns
            )

            # Detectar docentes EIB (BRP_TOTAL = 0)
            docentes_eib = len(df[df['BRP_TOTAL'] == 0]) if 'BRP_TOTAL' in df.columns else 0

            # Identificar columna de RBD
            rbd_col = None
            for col in df.columns:
                if 'rbd' in col.lower():
                    rbd_col = col
                    break

            total_establecimientos = df[rbd_col].nunique() if rbd_col else 0

            # Identificar columna de RUT
            rut_col = None
            for col in df.columns:
                if col == 'RUT_NORM' or 'rut' in col.lower():
                    rut_col = col
                    break

            total_docentes = df[rut_col].nunique() if rut_col else len(df)

            # Crear procesamiento
            procesamiento = ProcesamientoMensual(
                mes=mes,
                fecha_proceso=datetime.now(),
                total_docentes=total_docentes,
                total_establecimientos=total_establecimientos,
                brp_total=brp_total,
                brp_sep=brp_sep,
                brp_pie=brp_pie,
                brp_normal=brp_normal,
                reconocimiento_total=reconocimiento_total,
                tramo_total=tramo_total,
                docentes_eib=docentes_eib,
                notas=notas
            )
            session.add(procesamiento)
            session.flush()  # Para obtener el ID

            # Guardar docentes
            self._guardar_docentes(session, procesamiento.id, df, rut_col, rbd_col)

            session.commit()
            return procesamiento

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def _guardar_docentes(
        self,
        session: Session,
        procesamiento_id: int,
        df: pd.DataFrame,
        rut_col: str,
        rbd_col: str
    ) -> None:
        """Guarda los datos de docentes individuales."""
        # Identificar columnas de nombre
        nombre_col = None
        for col in df.columns:
            if 'nombre' in col.lower() and 'completo' in col.lower():
                nombre_col = col
                break
            if 'nombre' in col.lower():
                nombre_col = col

        tipo_pago_col = None
        for col in df.columns:
            if 'tipo' in col.lower() and 'pago' in col.lower():
                tipo_pago_col = col
                break

        tramo_col = None
        for col in df.columns:
            if col.lower() == 'tramo':
                tramo_col = col
                break

        for _, row in df.iterrows():
            rut = row.get(rut_col, '') if rut_col else ''
            if not rut:
                continue

            brp_total = row.get('BRP_TOTAL', 0) or 0

            docente = DocenteMensual(
                procesamiento_id=procesamiento_id,
                rut=str(rut),
                nombre=str(row.get(nombre_col, '')) if nombre_col else '',
                rbd=str(row.get(rbd_col, '')) if rbd_col else '',
                tipo_pago=str(row.get(tipo_pago_col, '')) if tipo_pago_col else '',
                tramo=str(row.get(tramo_col, '')) if tramo_col else '',
                brp_sep=row.get('BRP_SEP', 0) or 0,
                brp_pie=row.get('BRP_PIE', 0) or 0,
                brp_normal=row.get('BRP_NORMAL', 0) or 0,
                brp_total=brp_total,
                brp_reconocimiento_sep=row.get('BRP_RECONOCIMIENTO_SEP', 0) or 0,
                brp_reconocimiento_pie=row.get('BRP_RECONOCIMIENTO_PIE', 0) or 0,
                brp_reconocimiento_normal=row.get('BRP_RECONOCIMIENTO_NORMAL', 0) or 0,
                brp_tramo_sep=row.get('BRP_TRAMO_SEP', 0) or 0,
                brp_tramo_pie=row.get('BRP_TRAMO_PIE', 0) or 0,
                brp_tramo_normal=row.get('BRP_TRAMO_NORMAL', 0) or 0,
                es_eib=(brp_total == 0)
            )
            session.add(docente)

    def obtener_meses_disponibles(self) -> List[str]:
        """Obtiene lista de meses con procesamiento guardado."""
        session = self._get_session()
        try:
            procesamientos = session.query(ProcesamientoMensual.mes)\
                .order_by(desc(ProcesamientoMensual.mes))\
                .all()
            return [p.mes for p in procesamientos]
        finally:
            session.close()

    def obtener_procesamiento(self, mes: str) -> Optional[ProcesamientoMensual]:
        """Obtiene un procesamiento por mes."""
        session = self._get_session()
        try:
            return session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .first()
        finally:
            session.close()

    def obtener_datos_mes(self, mes: str) -> pd.DataFrame:
        """
        Obtiene los datos de docentes de un mes como DataFrame.

        Args:
            mes: Identificador del mes

        Returns:
            DataFrame con los datos de docentes
        """
        session = self._get_session()
        try:
            procesamiento = session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .first()

            if not procesamiento:
                return pd.DataFrame()

            docentes = session.query(DocenteMensual)\
                .filter_by(procesamiento_id=procesamiento.id)\
                .all()

            if not docentes:
                return pd.DataFrame()

            records = []
            for d in docentes:
                records.append({
                    'rut': d.rut,
                    'nombre': d.nombre,
                    'rbd': d.rbd,
                    'tipo_pago': d.tipo_pago,
                    'tramo': d.tramo,
                    'horas_sep': d.horas_sep,
                    'horas_pie': d.horas_pie,
                    'horas_sn': d.horas_sn,
                    'horas_total': d.horas_total,
                    'brp_sep': d.brp_sep,
                    'brp_pie': d.brp_pie,
                    'brp_normal': d.brp_normal,
                    'brp_total': d.brp_total,
                    'brp_reconocimiento_sep': d.brp_reconocimiento_sep,
                    'brp_reconocimiento_pie': d.brp_reconocimiento_pie,
                    'brp_reconocimiento_normal': d.brp_reconocimiento_normal,
                    'brp_tramo_sep': d.brp_tramo_sep,
                    'brp_tramo_pie': d.brp_tramo_pie,
                    'brp_tramo_normal': d.brp_tramo_normal,
                    'es_eib': d.es_eib
                })

            return pd.DataFrame(records)

        finally:
            session.close()

    def obtener_resumen_mes(self, mes: str) -> Optional[Dict[str, Any]]:
        """Obtiene resumen estadístico de un mes."""
        session = self._get_session()
        try:
            proc = session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .first()

            if not proc:
                return None

            return {
                'mes': proc.mes,
                'fecha_proceso': proc.fecha_proceso,
                'total_docentes': proc.total_docentes,
                'total_establecimientos': proc.total_establecimientos,
                'brp_total': proc.brp_total,
                'brp_sep': proc.brp_sep,
                'brp_pie': proc.brp_pie,
                'brp_normal': proc.brp_normal,
                'reconocimiento_total': proc.reconocimiento_total,
                'tramo_total': proc.tramo_total,
                'docentes_eib': proc.docentes_eib,
                'notas': proc.notas
            }
        finally:
            session.close()

    def eliminar_procesamiento(self, mes: str) -> bool:
        """Elimina un procesamiento y sus docentes asociados."""
        session = self._get_session()
        try:
            proc = session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .first()

            if proc:
                session.delete(proc)
                session.commit()
                return True
            return False
        except Exception:
            session.rollback()
            return False
        finally:
            session.close()

    def existe_mes(self, mes: str) -> bool:
        """Verifica si existe un procesamiento para el mes."""
        session = self._get_session()
        try:
            return session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .count() > 0
        finally:
            session.close()
