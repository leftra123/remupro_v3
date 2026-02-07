"""
Repositorio para operaciones CRUD sobre la base de datos de BRP.
"""

import re
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

from database.models import Base, ProcesamientoMensual, DocenteMensual, ColumnAlertPreference

# Strict pattern for month identifiers to prevent injection
_MES_PATTERN = re.compile(r"^\d{4}-\d{2}$")


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
        self._migrate()

    def _ensure_data_dir(self) -> None:
        """Crea el directorio data si no existe."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _migrate(self) -> None:
        """Add missing columns to existing tables (lightweight migration)."""
        from sqlalchemy import text, inspect as sa_inspect
        insp = sa_inspect(self.engine)
        cols = {c["name"] for c in insp.get_columns("procesamientos")}
        with self.engine.begin() as conn:
            if "daem_total" not in cols:
                conn.execute(text("ALTER TABLE procesamientos ADD COLUMN daem_total FLOAT DEFAULT 0"))
            if "cpeip_total" not in cols:
                conn.execute(text("ALTER TABLE procesamientos ADD COLUMN cpeip_total FLOAT DEFAULT 0"))

    def _get_session(self) -> Session:
        """Obtiene una sesión de base de datos."""
        return self.SessionLocal()

    def _validate_mes(self, mes: str) -> str:
        """Validate that mes matches YYYY-MM format to prevent injection."""
        mes = str(mes).strip()
        if not _MES_PATTERN.match(mes):
            raise ValueError(
                f"Formato de mes invalido: '{mes}'. Use YYYY-MM (ej: 2024-01)."
            )
        return mes

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
        mes = self._validate_mes(mes)
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

            # DAEM vs CPEIP totals
            daem_cols = ['TOTAL_DAEM_SEP', 'TOTAL_DAEM_PIE', 'TOTAL_DAEM_NORMAL']
            cpeip_cols = ['TOTAL_CPEIP_SEP', 'TOTAL_CPEIP_PIE', 'TOTAL_CPEIP_NORMAL']
            daem_total = sum(df[col].sum() for col in daem_cols if col in df.columns)
            cpeip_total = sum(df[col].sum() for col in cpeip_cols if col in df.columns)

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
        mes = self._validate_mes(mes)
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
        mes = self._validate_mes(mes)
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
        mes = self._validate_mes(mes)
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
        mes = self._validate_mes(mes)
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
        mes = self._validate_mes(mes)
        session = self._get_session()
        try:
            return session.query(ProcesamientoMensual)\
                .filter_by(mes=mes)\
                .count() > 0
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Column Alert Preferences
    # ------------------------------------------------------------------

    def guardar_preferencia_columna(self, columna_key: str, estado: str) -> ColumnAlertPreference:
        """Upsert preferencia de alerta para una columna."""
        if estado not in ('default', 'ignore', 'important'):
            raise ValueError(f"Estado invalido: '{estado}'. Use default/ignore/important.")
        session = self._get_session()
        try:
            pref = session.query(ColumnAlertPreference)\
                .filter_by(columna_key=columna_key)\
                .first()
            if pref:
                pref.estado = estado
            else:
                pref = ColumnAlertPreference(columna_key=columna_key, estado=estado)
                session.add(pref)
            session.commit()
            session.refresh(pref)
            return pref
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def obtener_preferencias_columnas(self) -> List[Dict[str, Any]]:
        """Lista todas las preferencias de columnas."""
        session = self._get_session()
        try:
            prefs = session.query(ColumnAlertPreference).all()
            return [
                {
                    'columna_key': p.columna_key,
                    'estado': p.estado,
                    'updated_at': p.updated_at.isoformat() if p.updated_at else None,
                }
                for p in prefs
            ]
        finally:
            session.close()

    def eliminar_preferencia_columna(self, columna_key: str) -> bool:
        """Elimina una preferencia (reset a default)."""
        session = self._get_session()
        try:
            pref = session.query(ColumnAlertPreference)\
                .filter_by(columna_key=columna_key)\
                .first()
            if pref:
                session.delete(pref)
                session.commit()
                return True
            return False
        except Exception:
            session.rollback()
            return False
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Teacher search & dashboard queries
    # ------------------------------------------------------------------

    def buscar_docentes(
        self,
        mes: str,
        query: str = "",
        rbd: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Busqueda paginada de docentes por RUT/nombre, filtro RBD."""
        mes = self._validate_mes(mes)
        session = self._get_session()
        try:
            proc = session.query(ProcesamientoMensual).filter_by(mes=mes).first()
            if not proc:
                return {"total": 0, "docentes": [], "limit": limit, "offset": offset}

            q = session.query(DocenteMensual).filter_by(procesamiento_id=proc.id)

            if query:
                pattern = f"%{query}%"
                q = q.filter(
                    (DocenteMensual.rut.ilike(pattern)) |
                    (DocenteMensual.nombre.ilike(pattern))
                )
            if rbd:
                q = q.filter(DocenteMensual.rbd == rbd)

            total = q.count()
            docentes = q.order_by(DocenteMensual.nombre)\
                .offset(offset).limit(limit).all()

            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "docentes": [
                    {
                        'rut': d.rut, 'nombre': d.nombre, 'rbd': d.rbd,
                        'tipo_pago': d.tipo_pago, 'tramo': d.tramo,
                        'horas_sep': d.horas_sep, 'horas_pie': d.horas_pie,
                        'horas_sn': d.horas_sn, 'horas_total': d.horas_total,
                        'brp_sep': d.brp_sep, 'brp_pie': d.brp_pie,
                        'brp_normal': d.brp_normal, 'brp_total': d.brp_total,
                        'es_eib': d.es_eib,
                    }
                    for d in docentes
                ],
            }
        finally:
            session.close()

    def obtener_escuelas(self, mes: str) -> List[Dict[str, Any]]:
        """Escuelas distintas con conteo de docentes y BRP total."""
        mes = self._validate_mes(mes)
        session = self._get_session()
        try:
            proc = session.query(ProcesamientoMensual).filter_by(mes=mes).first()
            if not proc:
                return []

            from sqlalchemy import func
            rows = session.query(
                DocenteMensual.rbd,
                func.count(DocenteMensual.id).label('docentes'),
                func.sum(DocenteMensual.brp_total).label('brp_total'),
                func.sum(DocenteMensual.brp_sep).label('brp_sep'),
                func.sum(DocenteMensual.brp_pie).label('brp_pie'),
                func.sum(DocenteMensual.brp_normal).label('brp_normal'),
            ).filter_by(procesamiento_id=proc.id)\
             .group_by(DocenteMensual.rbd)\
             .order_by(DocenteMensual.rbd)\
             .all()

            return [
                {
                    'rbd': r.rbd,
                    'docentes': r.docentes,
                    'brp_total': r.brp_total or 0,
                    'brp_sep': r.brp_sep or 0,
                    'brp_pie': r.brp_pie or 0,
                    'brp_normal': r.brp_normal or 0,
                }
                for r in rows
            ]
        finally:
            session.close()

    def obtener_tendencias(self) -> List[Dict[str, Any]]:
        """Series temporales de ProcesamientoMensual para grafico de tendencias."""
        session = self._get_session()
        try:
            procs = session.query(ProcesamientoMensual)\
                .order_by(ProcesamientoMensual.mes)\
                .all()
            return [
                {
                    'mes': p.mes,
                    'fecha_proceso': p.fecha_proceso.isoformat() if p.fecha_proceso else None,
                    'total_docentes': p.total_docentes,
                    'total_establecimientos': p.total_establecimientos,
                    'brp_total': p.brp_total,
                    'brp_sep': p.brp_sep,
                    'brp_pie': p.brp_pie,
                    'brp_normal': p.brp_normal,
                    'reconocimiento_total': p.reconocimiento_total,
                    'tramo_total': p.tramo_total,
                    'docentes_eib': p.docentes_eib,
                }
                for p in procs
            ]
        finally:
            session.close()

    def obtener_docentes_multi_establecimiento(self, mes: str) -> List[Dict[str, Any]]:
        """Docentes que aparecen en 2+ RBDs en un mes dado."""
        mes = self._validate_mes(mes)
        session = self._get_session()
        try:
            proc = session.query(ProcesamientoMensual).filter_by(mes=mes).first()
            if not proc:
                return []

            from sqlalchemy import func
            # Subquery: RUTs con 2+ RBDs distintos
            sub = session.query(DocenteMensual.rut)\
                .filter_by(procesamiento_id=proc.id)\
                .group_by(DocenteMensual.rut)\
                .having(func.count(func.distinct(DocenteMensual.rbd)) >= 2)\
                .subquery()

            docentes = session.query(DocenteMensual)\
                .filter_by(procesamiento_id=proc.id)\
                .filter(DocenteMensual.rut.in_(session.query(sub.c.rut)))\
                .order_by(DocenteMensual.rut, DocenteMensual.rbd)\
                .all()

            # Agrupar por RUT
            grouped: Dict[str, Dict[str, Any]] = {}
            for d in docentes:
                if d.rut not in grouped:
                    grouped[d.rut] = {
                        'rut': d.rut,
                        'nombre': d.nombre,
                        'establecimientos': [],
                        'total_brp': 0,
                        'total_horas': 0,
                    }
                grouped[d.rut]['establecimientos'].append({
                    'rbd': d.rbd,
                    'horas_sep': d.horas_sep,
                    'horas_pie': d.horas_pie,
                    'horas_sn': d.horas_sn,
                    'horas_total': d.horas_total,
                    'brp_sep': d.brp_sep,
                    'brp_pie': d.brp_pie,
                    'brp_normal': d.brp_normal,
                    'brp_total': d.brp_total,
                })
                grouped[d.rut]['total_brp'] += (d.brp_total or 0)
                grouped[d.rut]['total_horas'] += (d.horas_total or 0)

            return list(grouped.values())
        finally:
            session.close()
