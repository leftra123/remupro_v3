"""
Modelos SQLAlchemy para almacenamiento histórico de procesamiento BRP.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime,
    Boolean, ForeignKey, Text
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class ProcesamientoMensual(Base):
    """
    Registro de un procesamiento mensual de BRP.

    Almacena metadatos del procesamiento y permite
    relacionar con los docentes procesados.
    """
    __tablename__ = 'procesamientos'

    id = Column(Integer, primary_key=True)
    mes = Column(String(7), nullable=False, index=True)  # "2024-01"
    fecha_proceso = Column(DateTime, default=datetime.now)

    # Estadísticas generales
    total_docentes = Column(Integer, default=0)
    total_establecimientos = Column(Integer, default=0)

    # Montos totales
    brp_total = Column(Float, default=0)
    brp_sep = Column(Float, default=0)
    brp_pie = Column(Float, default=0)
    brp_normal = Column(Float, default=0)

    # Reconocimiento y Tramo
    reconocimiento_total = Column(Float, default=0)
    tramo_total = Column(Float, default=0)

    # DAEM vs CPEIP
    daem_total = Column(Float, default=0)
    cpeip_total = Column(Float, default=0)

    # Casos de revisión
    casos_revisar = Column(Integer, default=0)
    docentes_eib = Column(Integer, default=0)

    # Notas del procesamiento
    notas = Column(Text, nullable=True)

    # Relación con docentes
    docentes = relationship("DocenteMensual", back_populates="procesamiento",
                            cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ProcesamientoMensual(mes='{self.mes}', docentes={self.total_docentes})>"


class DocenteMensual(Base):
    """
    Datos de un docente en un procesamiento mensual específico.

    Permite comparar la situación de un docente entre meses.
    """
    __tablename__ = 'docentes_mensuales'

    id = Column(Integer, primary_key=True)
    procesamiento_id = Column(Integer, ForeignKey('procesamientos.id'), nullable=False)

    # Identificación
    rut = Column(String(12), nullable=False, index=True)
    nombre = Column(String(200), nullable=True)
    rbd = Column(String(10), nullable=True, index=True)

    # Tipo de pago y tramo
    tipo_pago = Column(String(50), nullable=True)
    tramo = Column(String(50), nullable=True)

    # Horas por tipo de subvención
    horas_sep = Column(Float, default=0)
    horas_pie = Column(Float, default=0)
    horas_sn = Column(Float, default=0)
    horas_total = Column(Float, default=0)

    # Montos BRP por tipo
    brp_sep = Column(Float, default=0)
    brp_pie = Column(Float, default=0)
    brp_normal = Column(Float, default=0)
    brp_total = Column(Float, default=0)

    # Desglose reconocimiento
    brp_reconocimiento_sep = Column(Float, default=0)
    brp_reconocimiento_pie = Column(Float, default=0)
    brp_reconocimiento_normal = Column(Float, default=0)

    # Desglose tramo
    brp_tramo_sep = Column(Float, default=0)
    brp_tramo_pie = Column(Float, default=0)
    brp_tramo_normal = Column(Float, default=0)

    # Flags especiales
    es_eib = Column(Boolean, default=False)
    excede_horas = Column(Boolean, default=False)
    requiere_revision = Column(Boolean, default=False)

    # Relación
    procesamiento = relationship("ProcesamientoMensual", back_populates="docentes")

    def __repr__(self):
        return f"<DocenteMensual(rut='{self.rut}', brp_total={self.brp_total})>"


class ProcesamientoAnual(Base):
    """
    Registro de un procesamiento anual de liquidaciones.
    """
    __tablename__ = 'procesamientos_anuales'

    id = Column(Integer, primary_key=True)
    anio = Column(Integer, nullable=False, index=True)
    fecha_proceso = Column(DateTime, default=datetime.now)

    total_docentes = Column(Integer, default=0)
    total_establecimientos = Column(Integer, default=0)
    total_registros = Column(Integer, default=0)

    brp_total_anual = Column(Float, default=0)
    haberes_total_anual = Column(Float, default=0)
    liquido_total_anual = Column(Float, default=0)

    notas = Column(Text, nullable=True)

    detalles = relationship("DocenteAnualDetalle", back_populates="procesamiento",
                            cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ProcesamientoAnual(anio={self.anio}, docentes={self.total_docentes})>"


class DocenteAnualDetalle(Base):
    """
    Datos de un docente en un mes del procesamiento anual.
    """
    __tablename__ = 'docentes_anuales_detalle'

    id = Column(Integer, primary_key=True)
    procesamiento_id = Column(Integer, ForeignKey('procesamientos_anuales.id'), nullable=False)

    rut = Column(String(12), nullable=False, index=True)
    nombre = Column(String(200), nullable=True)
    mes = Column(String(7), nullable=False, index=True)
    tipo_subvencion = Column(String(20), nullable=True)
    escuela = Column(String(200), nullable=True)
    rbd = Column(String(20), nullable=True, index=True)
    jornada = Column(Float, default=0)

    brp = Column(Float, default=0)
    sueldo_base = Column(Float, default=0)
    total_haberes = Column(Float, default=0)
    liquido_neto = Column(Float, default=0)
    monto_imponible = Column(Float, default=0)

    procesamiento = relationship("ProcesamientoAnual", back_populates="detalles")

    def __repr__(self):
        return f"<DocenteAnualDetalle(rut='{self.rut}', mes='{self.mes}', brp={self.brp})>"


class ColumnAlertPreference(Base):
    """
    Preferencia de alerta para columnas MINEDUC.

    Permite al usuario configurar si una columna faltante
    debe ser ignorada, marcada como importante, o usar el
    comportamiento por defecto.
    """
    __tablename__ = 'column_alert_preferences'

    id = Column(Integer, primary_key=True)
    columna_key = Column(String(100), nullable=False, unique=True, index=True)
    estado = Column(String(20), nullable=False, default='default')  # default, ignore, important
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<ColumnAlertPreference(columna_key='{self.columna_key}', estado='{self.estado}')>"
