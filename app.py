"""
RemuPro v2.3 - Sistema de Procesamiento de Remuneraciones Educativas
Interfaz Web con Streamlit
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile
from datetime import datetime

from processors import (
    SEPProcessor, PIEProcessor, DuplicadosProcessor,
    BRPProcessor, IntegradoProcessor, REMProcessor
)
from reports import AuditLog, InformeWord
from database import BRPRepository, ComparadorMeses
from config.columns import format_rut
import html as html_module
import json


def _sanitize_html(text: str) -> str:
    """Sanitize text for safe HTML rendering, preserving allowed formatting tags.

    Escapes all HTML entities first, then re-enables a small whitelist of
    formatting tags (<b>, <br>) that the application uses intentionally.
    This prevents XSS from user-controlled data (file names, error messages)
    while keeping the existing UI formatting intact.
    """
    safe = html_module.escape(str(text))
    # Re-enable only the specific safe tags used by the application
    safe = safe.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
    safe = safe.replace("&lt;br&gt;", "<br>").replace("&lt;br/&gt;", "<br/>")
    return safe


# ============================================================================
# CONFIGURACION
# ============================================================================

VERSION = "2.3.0"

st.set_page_config(
    page_title="RemuPro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

# ============================================================================
# ESTILOS CSS ADAPTATIVOS (Claro/Oscuro)
# ============================================================================

st.markdown("""
<style>
    /* ===== RESET Y BASE ===== */
    .block-container {
        padding: 2rem 3rem !important;
        max-width: 1200px;
    }
    
    /* ===== HEADER ===== */
    .app-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding-bottom: 1.5rem;
        margin-bottom: 1.5rem;
        border-bottom: 1px solid var(--text-color);
        opacity: 0.9;
    }
    
    .app-title {
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
    }
    
    .app-subtitle {
        font-size: 0.95rem;
        opacity: 0.7;
        margin: 0.25rem 0 0 0;
    }
    
    .app-version {
        font-size: 0.85rem;
        opacity: 0.5;
    }
    
    /* ===== CARDS ===== */
    .custom-card {
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.25rem;
        border: 1px solid rgba(128, 128, 128, 0.2);
        background: rgba(128, 128, 128, 0.05);
    }
    
    .card-header {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .card-content {
        font-size: 0.95rem;
        line-height: 1.6;
        opacity: 0.85;
    }
    
    /* ===== INFO BOXES ===== */
    .info-box {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        padding: 1rem 1.25rem;
        border-radius: 10px;
        margin-bottom: 1.25rem;
        font-size: 0.9rem;
    }
    
    .success-box {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        padding: 1rem 1.25rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-size: 0.95rem;
    }
    
    .warning-box {
        background: rgba(245, 158, 11, 0.15);
        border: 1px solid #f59e0b;
        color: #f59e0b;
        padding: 1rem 1.25rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-size: 0.9rem;
    }
    
    /* ===== TUTORIAL STEPS ===== */
    .tutorial-container {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
    }
    
    .tutorial-step {
        display: flex;
        align-items: flex-start;
        gap: 1rem;
        padding: 1rem;
        border-radius: 10px;
        background: rgba(128, 128, 128, 0.08);
        border-left: 4px solid #3b82f6;
    }
    
    .step-number {
        background: #3b82f6;
        color: white;
        min-width: 28px;
        height: 28px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 0.85rem;
    }
    
    .step-content {
        flex: 1;
    }
    
    .step-title {
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    
    .step-desc {
        font-size: 0.9rem;
        opacity: 0.8;
    }
    
    /* ===== TABS ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background: rgba(128, 128, 128, 0.1);
        padding: 0.5rem;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: #3b82f6 !important;
        color: white !important;
    }
    
    /* ===== BOTONES ===== */
    .stButton > button {
        width: 100%;
        padding: 0.875rem 1.5rem;
        font-weight: 600;
        font-size: 1rem;
        border-radius: 10px;
        border: none;
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        transition: all 0.2s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(16, 185, 129, 0.3);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    .stDownloadButton > button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
        color: white !important;
        border: none !important;
        width: 100%;
        padding: 0.875rem 1.5rem;
        font-weight: 600;
        border-radius: 10px;
    }
    
    .stDownloadButton > button:hover {
        box-shadow: 0 8px 20px rgba(59, 130, 246, 0.3);
    }
    
    /* ===== FILE UPLOADER ===== */
    [data-testid="stFileUploader"] {
        border-radius: 10px;
    }
    
    [data-testid="stFileUploader"] section {
        border: 2px dashed rgba(128, 128, 128, 0.3);
        border-radius: 10px;
        padding: 1.5rem;
        transition: all 0.2s ease;
    }
    
    [data-testid="stFileUploader"] section:hover {
        border-color: #3b82f6;
        background: rgba(59, 130, 246, 0.05);
    }
    
    /* ===== METRICS ===== */
    [data-testid="stMetricValue"] {
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        color: #3b82f6 !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-weight: 500;
    }
    
    /* ===== EXPANDER ===== */
    .streamlit-expanderHeader {
        font-weight: 600;
        font-size: 1rem;
        border-radius: 10px;
    }
    
    /* ===== SELECTBOX ===== */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
    
    /* ===== PROGRESS ===== */
    .stProgress > div > div {
        background: linear-gradient(90deg, #3b82f6 0%, #10b981 100%);
        border-radius: 10px;
    }
    
    /* ===== DATAFRAME ===== */
    [data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
    }
    
    /* ===== FOOTER ===== */
    .app-footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        margin-top: 2rem;
        border-top: 1px solid rgba(128, 128, 128, 0.2);
        font-size: 0.85rem;
        opacity: 0.6;
    }
    
    /* ===== AUDIT LOG ===== */
    .audit-summary-bar {
        display: flex;
        gap: 1rem;
        padding: 0.75rem 1rem;
        border-radius: 10px;
        background: rgba(128, 128, 128, 0.08);
        border: 1px solid rgba(128, 128, 128, 0.15);
        margin-bottom: 1rem;
        flex-wrap: wrap;
        align-items: center;
    }
    .audit-summary-bar .audit-stat {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.9rem;
        font-weight: 500;
    }
    .audit-badge {
        display: inline-block;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        color: white;
        line-height: 1.4;
    }
    .audit-badge-error { background: #ef4444; }
    .audit-badge-warning { background: #f59e0b; }
    .audit-badge-info { background: #3b82f6; }
    .audit-badge-tipo {
        background: rgba(128, 128, 128, 0.15);
        color: inherit;
        font-weight: 500;
    }
    .audit-group-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 0;
        font-weight: 600;
        font-size: 0.95rem;
        border-bottom: 1px solid rgba(128, 128, 128, 0.15);
        margin-bottom: 0.5rem;
    }
    .audit-entry {
        display: flex;
        gap: 0.75rem;
        padding: 0.4rem 0.5rem;
        border-radius: 6px;
        font-size: 0.85rem;
        align-items: flex-start;
    }
    .audit-entry:hover {
        background: rgba(128, 128, 128, 0.06);
    }
    .audit-entry .audit-time {
        opacity: 0.5;
        min-width: 65px;
        font-family: monospace;
        font-size: 0.8rem;
    }
    .audit-entry .audit-msg {
        flex: 1;
        line-height: 1.4;
    }
    .audit-duration {
        font-size: 0.85rem;
        opacity: 0.6;
        margin-top: 0.5rem;
        font-style: italic;
    }

    /* ===== OCULTAR ELEMENTOS DE STREAMLIT ===== */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* ===== ANIMACIONES ===== */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .custom-card {
        animation: fadeIn 0.3s ease-out;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def show_header():
    """Muestra el header de la aplicación."""
    st.markdown(f"""
    <div class="app-header">
        <div>
            <h1 class="app-title">📊 RemuPro</h1>
            <p class="app-subtitle">Sistema de Procesamiento de Remuneraciones Educativas</p>
        </div>
        <div class="app-version">v{VERSION}</div>
    </div>
    """, unsafe_allow_html=True)


def info_box(text: str):
    """Muestra una caja de información (HTML-sanitized)."""
    safe_text = _sanitize_html(text)
    st.markdown(f'<div class="info-box">{safe_text}</div>', unsafe_allow_html=True)


def success_box(text: str):
    """Muestra una caja de éxito (HTML-sanitized)."""
    safe_text = _sanitize_html(text)
    st.markdown(f'<div class="success-box">{safe_text}</div>', unsafe_allow_html=True)


def warning_box(text: str):
    """Muestra una caja de advertencia (HTML-sanitized)."""
    safe_text = _sanitize_html(text)
    st.markdown(f'<div class="warning-box">{safe_text}</div>', unsafe_allow_html=True)


def show_audit_log_detailed(audit):
    """
    Muestra el log de auditoría con resumen visual, agrupado por tipo,
    ordenado por severidad (errores primero), y con info de duración.
    """
    if not audit or len(audit) == 0:
        return

    summary = audit.get_summary()
    total = summary.get('total', 0)
    n_errors = summary.get('errores', 0)
    n_warnings = summary.get('advertencias', 0)
    n_info = total - n_errors - n_warnings

    # --- Summary bar ---
    parts = [f'<span class="audit-stat">Total: <b>{total}</b></span>']
    if n_errors:
        parts.append(
            f'<span class="audit-stat"><span class="audit-badge audit-badge-error">'
            f'ERROR</span> {n_errors}</span>'
        )
    if n_warnings:
        parts.append(
            f'<span class="audit-stat"><span class="audit-badge audit-badge-warning">'
            f'WARNING</span> {n_warnings}</span>'
        )
    if n_info:
        parts.append(
            f'<span class="audit-stat"><span class="audit-badge audit-badge-info">'
            f'INFO</span> {n_info}</span>'
        )

    # Duration
    duration_text = ""
    proceso_entries = audit.get_by_tipo(audit.TIPO_PROCESO)
    if len(proceso_entries) >= 2:
        t_start = proceso_entries[0].timestamp
        t_end = proceso_entries[-1].timestamp
        secs = (t_end - t_start).total_seconds()
        duration_text = f' <span class="audit-stat" style="margin-left:auto;">Duracion: <b>{secs:.1f}s</b></span>'

    st.markdown(
        f'<div class="audit-summary-bar">{"".join(parts)}{duration_text}</div>',
        unsafe_allow_html=True
    )

    # --- Build sorted entries: errors > warnings > info ---
    nivel_order = {'ERROR': 0, 'WARNING': 1, 'INFO': 2}
    sorted_entries = sorted(
        audit.entries,
        key=lambda e: (nivel_order.get(e.nivel, 3), e.timestamp)
    )

    # --- Friendly type names ---
    tipo_labels = {
        'columna_faltante': 'Columnas Faltantes',
        'valor_inusual': 'Valores Inusuales',
        'docente_eib': 'Docentes EIB',
        'excede_horas': 'Excede Horas',
        'sin_liquidacion': 'Sin Liquidacion',
        'validacion': 'Validacion',
        'proceso': 'Proceso',
        'archivo': 'Archivo',
    }

    badge_class = {
        'ERROR': 'audit-badge-error',
        'WARNING': 'audit-badge-warning',
        'INFO': 'audit-badge-info',
    }

    # --- Expander with grouped entries ---
    label = "Ver log de auditoria detallado"
    if n_errors:
        label = f"Log de auditoria ({n_errors} errores, {n_warnings} advertencias)"
    elif n_warnings:
        label = f"Log de auditoria ({n_warnings} advertencias)"

    with st.expander(label):
        # Filter
        niveles_disponibles = sorted(
            set(e.nivel for e in audit.entries),
            key=lambda n: nivel_order.get(n, 3)
        )
        filtro = st.multiselect(
            "Filtrar por nivel",
            options=niveles_disponibles,
            default=niveles_disponibles,
            key="audit_log_filter"
        )

        filtered = [e for e in sorted_entries if e.nivel in filtro]

        if not filtered:
            st.info("No hay entradas con los filtros seleccionados.")
            return

        # Group by tipo
        from collections import OrderedDict
        groups = OrderedDict()
        for entry in filtered:
            tipo_key = entry.tipo
            if tipo_key not in groups:
                groups[tipo_key] = []
            groups[tipo_key].append(entry)

        for tipo_key, entries in groups.items():
            tipo_label = tipo_labels.get(tipo_key, tipo_key.replace('_', ' ').title())
            # Determine worst level in group for header badge
            worst = 'INFO'
            for e in entries:
                if e.nivel == 'ERROR':
                    worst = 'ERROR'
                    break
                if e.nivel == 'WARNING':
                    worst = 'WARNING'

            badge_cls = badge_class.get(worst, 'audit-badge-info')
            header_html = (
                f'<div class="audit-group-header">'
                f'<span class="audit-badge {badge_cls}">{worst}</span> '
                f'{tipo_label} ({len(entries)})'
                f'</div>'
            )
            st.markdown(header_html, unsafe_allow_html=True)

            # Render entries (limit to 50 per group)
            display_entries = entries[:50]
            rows_html = []
            for e in display_entries:
                time_str = e.timestamp.strftime("%H:%M:%S")
                bcls = badge_class.get(e.nivel, 'audit-badge-info')
                rows_html.append(
                    f'<div class="audit-entry">'
                    f'<span class="audit-time">{time_str}</span>'
                    f'<span class="audit-badge {bcls}">{e.nivel}</span>'
                    f'<span class="audit-msg">{e.mensaje}</span>'
                    f'</div>'
                )
            st.markdown("".join(rows_html), unsafe_allow_html=True)

            if len(entries) > 50:
                st.caption(f"... y {len(entries) - 50} entradas mas en este grupo")

        # Table view
        st.markdown("---")
        st.markdown("**Tabla completa**")
        df_audit = pd.DataFrame([
            {
                'Hora': e.timestamp.strftime("%H:%M:%S"),
                'Nivel': e.nivel,
                'Tipo': tipo_labels.get(e.tipo, e.tipo),
                'Mensaje': e.mensaje,
            }
            for e in filtered
        ])
        st.dataframe(df_audit, use_container_width=True, hide_index=True)


def format_user_error(e: Exception) -> str:
    """Traduce excepciones técnicas a mensajes amigables en español."""
    error_str = str(e)
    error_type = type(e).__name__

    if error_type == 'ColumnMissingError' or 'columna' in error_str.lower():
        return "El archivo no tiene las columnas esperadas. Verifique que usa el archivo correcto."
    elif error_type == 'FileValidationError' or 'validación' in error_str.lower():
        if 'vacío' in error_str.lower() or 'empty' in error_str.lower():
            return "El archivo está vacío. Verifique que el archivo tiene datos."
        elif 'formato' in error_str.lower() or 'format' in error_str.lower():
            return "El formato del archivo no es válido. Use un archivo Excel (.xlsx)."
        elif 'no encontr' in error_str.lower() or 'not found' in error_str.lower():
            return "No se encontró el archivo. Vuelva a cargarlo."
        return f"Error de validación: {error_str}"
    elif isinstance(e, PermissionError) or 'permission' in error_str.lower():
        return "El archivo está abierto en otro programa. Cierre el archivo en Excel e intente nuevamente."
    else:
        return f"Ocurrió un error. Detalle: {error_str}"


@st.cache_data
def load_escuelas():
    """Carga mapa de RBD → nombre de escuela desde config/escuelas.json."""
    try:
        escuelas_path = Path(__file__).parent / 'config' / 'escuelas.json'
        with open(escuelas_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def add_school_names(df, rbd_col='RBD'):
    """Agrega columna ESCUELA al DataFrame basándose en el RBD."""
    escuelas = load_escuelas()
    if not escuelas or rbd_col not in df.columns:
        return df
    df = df.copy()
    df['ESCUELA'] = df[rbd_col].astype(str).str.replace(r'\.0$', '', regex=True).map(
        lambda x: escuelas.get(str(x).split('.')[0].split('-')[0].strip(), '')
    )
    # Mover ESCUELA después de RBD
    cols = list(df.columns)
    if 'ESCUELA' in cols and rbd_col in cols:
        cols.remove('ESCUELA')
        idx = cols.index(rbd_col) + 1
        cols.insert(idx, 'ESCUELA')
        df = df[cols]
    return df


def show_column_alerts(column_alerts):
    """Muestra alertas de columnas con lista expandible para columnas nuevas."""
    if not column_alerts:
        return
    criticas = [a for a in column_alerts if a['nivel'] == 'error']
    nuevas = [a for a in column_alerts if a['tipo'] == 'columna_nueva']

    if criticas:
        for a in criticas:
            warning_box(f"<b>Columna no encontrada:</b> {a['columna_nombre']}<br>"
                        f"Los montos de este concepto serán $0.")
    if nuevas:
        for a in nuevas:
            lista = a.get('columnas_lista', [])
            if lista:
                info_box(f"<b>Columnas nuevas detectadas ({len(lista)}):</b> "
                         f"El archivo MINEDUC tiene columnas que el sistema no procesa.")
                with st.expander(f"Ver las {len(lista)} columnas nuevas"):
                    for col in lista:
                        st.markdown(f"- `{col}`")
                    st.caption("Para registrar estas columnas, agregarlas en "
                               "`config/columns.py` → `WEB_SOSTENEDOR_COLUMNS`")


def show_desglose_daem_cpeip(df, prefix=''):
    """Muestra desglose DAEM/CPEIP con totales detallados por concepto."""
    cols = df.columns

    daem_recon = sum(df[f'DAEM_RECON_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'DAEM_RECON_{s}' in cols)
    daem_tramo = sum(df[f'DAEM_TRAMO_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'DAEM_TRAMO_{s}' in cols)
    daem_total = daem_recon + daem_tramo

    cpeip_recon = sum(df[f'CPEIP_RECON_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'CPEIP_RECON_{s}' in cols)
    cpeip_tramo = sum(df[f'CPEIP_TRAMO_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'CPEIP_TRAMO_{s}' in cols)
    cpeip_prior = sum(df[f'CPEIP_PRIOR_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'CPEIP_PRIOR_{s}' in cols)
    cpeip_total = cpeip_recon + cpeip_tramo + cpeip_prior

    st.markdown("##### 🏛️ Desglose por Pagador")
    st.caption("DAEM = lo que paga el municipio (Subvención) | CPEIP = lo que transfiere el ministerio (Transferencia)")

    col_daem, col_cpeip = st.columns(2)
    with col_daem:
        st.metric("Total DAEM (Subvención)", f"${daem_total:,.0f}")
        c1, c2 = st.columns(2)
        c1.metric("DAEM Reconocimiento", f"${daem_recon:,.0f}")
        c2.metric("DAEM Tramo", f"${daem_tramo:,.0f}")
    with col_cpeip:
        st.metric("Total CPEIP (Transferencia)", f"${cpeip_total:,.0f}")
        c1, c2, c3 = st.columns(3)
        c1.metric("CPEIP Reconocimiento", f"${cpeip_recon:,.0f}")
        c2.metric("CPEIP Tramo", f"${cpeip_tramo:,.0f}")
        c3.metric("CPEIP Prioritarios", f"${cpeip_prior:,.0f}")


def show_revision_table(df_revision):
    """Muestra tabla de revisión con RUT formateado y nombre/apellido visibles."""
    if df_revision.empty:
        return

    # Formatear RUT con guión
    if 'RUT' in df_revision.columns:
        df_revision = df_revision.copy()
        df_revision['RUT'] = df_revision['RUT'].apply(format_rut)

    # Asegurar que NOMBRE y APELLIDOS estén visibles al inicio
    cols_prioritarias = ['RUT', 'NOMBRE', 'APELLIDOS', 'TIPO_PAGO', 'MOTIVO',
                         'HORAS_SEP', 'HORAS_PIE', 'HORAS_SN', 'HORAS_TOTAL',
                         'EXCESO', 'DETALLE', 'ACCION']
    cols_exist = [c for c in cols_prioritarias if c in df_revision.columns]
    cols_resto = [c for c in df_revision.columns if c not in cols_exist]
    df_revision = df_revision[cols_exist + cols_resto]

    st.dataframe(df_revision, use_container_width=True, hide_index=True)


def show_charts_by_school(df_rbd):
    """Muestra gráficos de distribución BRP por escuela."""
    import plotly.express as px
    import plotly.graph_objects as go

    # Agregar nombres de escuela
    df_rbd = add_school_names(df_rbd)

    # Excluir fila TOTAL
    df_chart = df_rbd[df_rbd['RBD'].astype(str) != 'TOTAL'].copy()
    if df_chart.empty:
        return

    # Etiqueta para eje: ESCUELA o RBD
    if 'ESCUELA' in df_chart.columns:
        df_chart['LABEL'] = df_chart.apply(
            lambda r: r['ESCUELA'] if r['ESCUELA'] else str(r['RBD']), axis=1
        )
    else:
        df_chart['LABEL'] = df_chart['RBD'].astype(str)

    # Gráfico de barras apiladas: SEP/PIE/NORMAL por escuela
    fig = go.Figure()
    for subv, color in [('BRP_SEP', '#3b82f6'), ('BRP_PIE', '#10b981'), ('BRP_NORMAL', '#f59e0b')]:
        if subv in df_chart.columns:
            fig.add_trace(go.Bar(
                name=subv.replace('BRP_', ''),
                x=df_chart['LABEL'],
                y=df_chart[subv],
                marker_color=color,
                text=df_chart[subv].apply(lambda x: f"${x:,.0f}"),
                textposition='inside'
            ))
    fig.update_layout(
        barmode='stack',
        title='BRP Total por Escuela (SEP / PIE / Normal)',
        xaxis_title='',
        yaxis_title='Monto ($)',
        height=400,
        xaxis_tickangle=-35,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # Gráfico DAEM vs CPEIP por escuela
    daem_cols = ['DAEM_SEP', 'DAEM_PIE', 'DAEM_NORMAL']
    cpeip_cols = ['CPEIP_SEP', 'CPEIP_PIE', 'CPEIP_NORMAL']
    has_daem = all(c in df_chart.columns for c in daem_cols)
    has_cpeip = all(c in df_chart.columns for c in cpeip_cols)

    if has_daem and has_cpeip:
        df_chart['TOTAL_DAEM'] = df_chart[daem_cols].sum(axis=1)
        df_chart['TOTAL_CPEIP'] = df_chart[cpeip_cols].sum(axis=1)

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name='DAEM', x=df_chart['LABEL'], y=df_chart['TOTAL_DAEM'],
            marker_color='#6366f1',
            text=df_chart['TOTAL_DAEM'].apply(lambda x: f"${x:,.0f}"),
            textposition='inside'
        ))
        fig2.add_trace(go.Bar(
            name='CPEIP', x=df_chart['LABEL'], y=df_chart['TOTAL_CPEIP'],
            marker_color='#ec4899',
            text=df_chart['TOTAL_CPEIP'].apply(lambda x: f"${x:,.0f}"),
            textposition='inside'
        ))
        fig2.update_layout(
            barmode='group',
            title='DAEM vs CPEIP por Escuela',
            xaxis_title='',
            yaxis_title='Monto ($)',
            height=400,
            xaxis_tickangle=-35,
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
        )
        st.plotly_chart(fig2, use_container_width=True)


def show_rem_results(df_resumen, alertas):
    """Muestra resultados del procesamiento REM (horas disponibles)."""
    st.markdown("##### 📋 Análisis de Horas (REM)")

    # Métricas generales
    total_personas = len(df_resumen)
    exceden = df_resumen['EXCEDE'].sum() if 'EXCEDE' in df_resumen.columns else 0
    total_horas = df_resumen['TOTAL'].sum() if 'TOTAL' in df_resumen.columns else 0
    disponibles = df_resumen['DISPONIBLE'].sum() if 'DISPONIBLE' in df_resumen.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Personas", f"{total_personas}")
    with c2:
        st.metric("Total Horas", f"{int(total_horas)}")
    with c3:
        st.metric("Horas Disponibles", f"{int(disponibles)}")
    with c4:
        st.metric("Exceden 44 hrs", f"{int(exceden)}")
        if exceden > 0:
            st.badge(f"{exceden} alerta(s)", color="red")

    # Alertas de exceso
    if alertas:
        st.markdown("---")
        st.markdown(f"##### ⚠️ {len(alertas)} persona(s) exceden 44 horas")
        for a in alertas:
            warning_box(
                f"<b>{a.get('nombre', a['rut'])}</b> — {a['detalle']}<br>"
                f"Exceso: <b>{a['exceso']} hrs</b>"
            )

    # Tabla resumen con RUT formateado
    st.markdown("---")
    st.markdown("##### 📊 Detalle por persona")

    df_display = df_resumen.copy()
    if 'RUT_NORM' in df_display.columns:
        df_display['RUT'] = df_display['RUT_NORM'].apply(format_rut)
        cols = ['RUT'] + [c for c in df_display.columns if c not in ('RUT', 'RUT_NORM')]
        df_display = df_display[cols]
        df_display = df_display.drop(columns=['RUT_NORM'], errors='ignore')

    # Resaltar filas que exceden
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # Resumen por tipo
    if total_horas > 0:
        import plotly.express as px
        horas_sep = df_resumen['SEP'].sum() if 'SEP' in df_resumen.columns else 0
        horas_pie = df_resumen['PIE'].sum() if 'PIE' in df_resumen.columns else 0
        horas_normal = df_resumen['NORMAL'].sum() if 'NORMAL' in df_resumen.columns else 0
        horas_eib = df_resumen['EIB'].sum() if 'EIB' in df_resumen.columns else 0

        tipos = ['SEP', 'PIE', 'Normal', 'EIB']
        vals = [horas_sep, horas_pie, horas_normal, horas_eib]
        # Filtrar los que tienen valor
        data = [(t, v) for t, v in zip(tipos, vals) if v > 0]
        if data:
            df_h = pd.DataFrame(data, columns=['Tipo', 'Horas'])
            fig = px.pie(df_h, values='Horas', names='Tipo',
                         title='Distribución de Horas por Tipo',
                         color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


def show_multi_establishment(out_path):
    """Muestra sección de docentes multi-establecimiento."""
    try:
        df_multi = pd.read_excel(out_path, sheet_name='MULTI_ESTABLECIMIENTO', engine='openpyxl')
    except Exception:
        return

    if df_multi.empty:
        return

    total_docentes = df_multi[df_multi['TIPO_FILA'] == 'TOTAL_DOCENTE']
    n_docentes = len(total_docentes)

    st.markdown("---")
    st.markdown("##### 🏫 Docentes Multi-Establecimiento")
    st.caption(f"{n_docentes} docente(s) trabajan en 2 o más escuelas")

    if n_docentes == 0:
        st.info("No hay docentes en múltiples establecimientos.")
        return

    # Agregar nombres de escuela a los RBD
    escuelas = load_escuelas()

    # Formatear RUT
    df_multi = df_multi.copy()
    df_multi['RUT'] = df_multi['RUT'].apply(format_rut)

    # Agregar nombre escuela
    if escuelas:
        df_multi['ESCUELA'] = df_multi['RBD'].astype(str).map(
            lambda x: escuelas.get(str(x).split('.')[0].split('-')[0].strip(), '') if x != 'TOTAL' else ''
        )

    # Mostrar por docente con expanders
    for _, total_row in total_docentes.iterrows():
        rut = total_row['RUT']
        nombre = total_row['NOMBRE']
        brp_total = total_row['BRP_TOTAL']

        detalle = df_multi[(df_multi['RUT'] == rut) & (df_multi['TIPO_FILA'] == 'DETALLE')]

        with st.expander(f"**{nombre}** ({rut}) — BRP Total: ${brp_total:,.0f} — {len(detalle)} escuelas"):
            # Tabla de desglose
            cols_show = ['RBD']
            if 'ESCUELA' in detalle.columns:
                cols_show.append('ESCUELA')
            cols_show += ['HORAS_CONTRATO', 'RECONOCIMIENTO_MINEDUC', 'TRAMO_MINEDUC',
                          'PRIORITARIOS_MINEDUC', 'BRP_SEP', 'BRP_PIE', 'BRP_NORMAL', 'BRP_TOTAL']
            cols_show = [c for c in cols_show if c in detalle.columns]
            st.dataframe(detalle[cols_show], use_container_width=True, hide_index=True)

            # Métricas comparativas
            if len(detalle) >= 2:
                chart_cols = st.columns(len(detalle))
                for i, (_, row) in enumerate(detalle.iterrows()):
                    rbd_label = row.get('ESCUELA', str(row['RBD'])) if 'ESCUELA' in detalle.columns else str(row['RBD'])
                    if not rbd_label:
                        rbd_label = str(row['RBD'])
                    with chart_cols[i]:
                        st.metric(f"RBD {row['RBD']}", f"${row['BRP_TOTAL']:,.0f}")
                        st.caption(rbd_label[:25])
                        st.caption(f"Hrs: {row['HORAS_CONTRATO']}")

    # Resumen general
    st.markdown("**Totales por docente multi-establecimiento:**")
    cols_total = ['RUT', 'NOMBRE', 'HORAS_CONTRATO', 'RECONOCIMIENTO_MINEDUC',
                  'TRAMO_MINEDUC', 'PRIORITARIOS_MINEDUC', 'BRP_TOTAL']
    cols_total = [c for c in cols_total if c in total_docentes.columns]
    st.dataframe(total_docentes[cols_total], use_container_width=True, hide_index=True)


def show_sidebar_charts():
    """Muestra charts dinámicos en el sidebar basados en resultados del procesamiento."""
    if 'last_brp_result' not in st.session_state:
        return

    data = st.session_state['last_brp_result']
    mes = data.get('mes', '')
    brp_sep = data.get('brp_sep', 0)
    brp_pie = data.get('brp_pie', 0)
    brp_normal = data.get('brp_normal', 0)
    brp_total = data.get('brp_total', 0)
    daem_total = data.get('daem_total', 0)
    cpeip_total = data.get('cpeip_total', 0)

    if brp_total <= 0:
        return

    import plotly.graph_objects as go

    st.markdown(f"**📅 Mes: {mes}**")
    st.metric("BRP Total", f"${brp_total:,.0f}")

    # Mini gráfico de torta SEP/PIE/Normal
    fig = go.Figure(data=[go.Pie(
        labels=['SEP', 'PIE', 'Normal'],
        values=[brp_sep, brp_pie, brp_normal],
        marker_colors=['#3b82f6', '#10b981', '#f59e0b'],
        textinfo='percent',
        hole=0.4,
    )])
    fig.update_layout(
        height=200, margin=dict(l=10, r=10, t=25, b=10),
        title_text='SEP / PIE / Normal', title_font_size=12,
        showlegend=True,
        legend=dict(font=dict(size=10), orientation='h', y=-0.1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # DAEM vs CPEIP
    if daem_total > 0 or cpeip_total > 0:
        fig2 = go.Figure(data=[go.Bar(
            x=['DAEM', 'CPEIP'],
            y=[daem_total, cpeip_total],
            marker_color=['#6366f1', '#ec4899'],
            text=[f"${daem_total:,.0f}", f"${cpeip_total:,.0f}"],
            textposition='inside',
        )])
        fig2.update_layout(
            height=200, margin=dict(l=10, r=10, t=25, b=10),
            title_text='DAEM vs CPEIP', title_font_size=12,
            showlegend=False, yaxis_visible=False,
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Detalle por tipo
    st.caption(f"SEP: ${brp_sep:,.0f}")
    st.caption(f"PIE: ${brp_pie:,.0f}")
    st.caption(f"Normal: ${brp_normal:,.0f}")

    # Info REM si existe
    if 'last_rem_result' in st.session_state:
        rem = st.session_state['last_rem_result']
        st.markdown("---")
        st.markdown("**📋 REM**")
        st.caption(f"Personas: {rem.get('personas', 0)}")
        st.caption(f"Horas totales: {rem.get('total_horas', 0)}")
        exceden = rem.get('exceden', 0)
        if exceden > 0:
            st.badge(f"{exceden} exceden 44hrs", color="red")
        else:
            st.badge("Todos dentro del límite", color="green")


def card_start(title: str, icon: str = ""):
    """Inicia una tarjeta (HTML-sanitized)."""
    safe_title = _sanitize_html(title)
    safe_icon = _sanitize_html(icon)
    st.markdown(f"""
    <div class="custom-card">
        <div class="card-header">{safe_icon} {safe_title}</div>
        <div class="card-content">
    """, unsafe_allow_html=True)


def card_end():
    """Cierra una tarjeta."""
    st.markdown('</div></div>', unsafe_allow_html=True)


def show_tutorial(steps: list):
    """Muestra pasos del tutorial (HTML-sanitized)."""
    html = '<div class="tutorial-container">'
    for i, (title, desc) in enumerate(steps, 1):
        safe_title = _sanitize_html(title)
        safe_desc = _sanitize_html(desc)
        html += f'''
        <div class="tutorial-step">
            <div class="step-number">{i}</div>
            <div class="step-content">
                <div class="step-title">{safe_title}</div>
                <div class="step-desc">{safe_desc}</div>
            </div>
        </div>
        '''
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def check_sheets(file, required: list) -> tuple:
    """Valida hojas del Excel."""
    try:
        xlsx = pd.ExcelFile(file)
        missing = [s for s in required if s not in xlsx.sheet_names]
        return len(missing) == 0, missing
    except Exception as e:
        return False, [str(e)]


def to_excel_buffer(df: pd.DataFrame) -> BytesIO:
    """Convierte DataFrame a buffer para descarga."""
    buf = BytesIO()
    df.to_excel(buf, index=False, engine='openpyxl')
    buf.seek(0)
    return buf


def _cleanup_temp_files(*paths):
    """Securely remove temporary files containing sensitive salary data."""
    import os
    for p in paths:
        try:
            if p and Path(p).exists():
                os.unlink(p)
        except OSError:
            pass


def process_files(processor, inputs: list):
    """Procesa archivos con barra de progreso. Cleans up temp files after use."""
    progress = st.progress(0)
    status = st.empty()

    def callback(val, msg):
        progress.progress(val / 100)
        status.markdown(f"**{msg}**")

    paths = []
    out_path = None
    try:
        # Crear archivos temporales
        for f in inputs:
            tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            tmp.write(f.getvalue())
            paths.append(Path(tmp.name))
            tmp.close()

        out_tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        out_path = Path(out_tmp.name)
        out_tmp.close()

        # Ejecutar procesador segun cantidad de archivos
        if len(paths) == 1:
            processor.process_file(paths[0], out_path, callback)
        elif len(paths) == 2:
            processor.process_file(paths[0], paths[1], out_path, callback)
        elif len(paths) == 3:
            processor.process_file(
                web_sostenedor_path=paths[0],
                sep_procesado_path=paths[1],
                pie_procesado_path=paths[2],
                output_path=out_path,
                progress_callback=callback
            )

        df = pd.read_excel(out_path, engine='openpyxl')
        progress.progress(100)
        status.markdown("**Completado**")
        return df, None

    except Exception as e:
        return None, format_user_error(e)
    finally:
        # Always clean up temp files to prevent sensitive data leaks
        _cleanup_temp_files(*paths, out_path)


# ============================================================================
# PESTAÑAS
# ============================================================================

def tab_sep_pie():
    """Pestaña de procesamiento SEP/PIE."""
    
    with st.expander("📖 ¿Cómo usar esta herramienta?", expanded=False):
        show_tutorial([
            ("Selecciona el tipo", "Elige SEP o PIE-NORMAL según el archivo a procesar."),
            ("Sube el archivo", "Debe ser un Excel con las hojas HORAS y TOTAL."),
            ("Procesa", "Haz clic en el botón verde y espera."),
            ("Descarga", "Guarda el archivo procesado en tu computador.")
        ])
    
    st.markdown("---")
    
    # Configuración
    col1, col2 = st.columns([1, 2])
    with col1:
        modo = st.selectbox(
            "📋 Tipo de procesamiento",
            ["SEP", "PIE-NORMAL"],
            help="SEP: Subvención Escolar Preferencial\nPIE-NORMAL: Programa de Integración + Subvención Normal"
        )
    
    st.markdown("")
    
    # Archivo
    st.markdown("##### 📁 Archivo de Entrada")
    st.caption("El archivo debe contener las hojas **HORAS** y **TOTAL**")
    archivo = st.file_uploader(
        "Arrastra o selecciona un archivo Excel",
        type=['xlsx', 'xls'],
        key="sep_file"
    )
    
    if archivo:
        ok, missing = check_sheets(archivo, ['HORAS', 'TOTAL'])
        if not ok:
            st.error(f"❌ **Archivo incorrecto** - Faltan hojas: {', '.join(missing)}")
            return
        
        st.success(f"✅ **{archivo.name}** - Archivo válido")
        
        st.markdown("")
        
        if st.button("▶️  PROCESAR ARCHIVO", key="btn_sep", use_container_width=True):
            processor = SEPProcessor() if modo == "SEP" else PIEProcessor()
            df, error = process_files(processor, [archivo])
            
            if error:
                st.error(f"❌ **Error:** {error}")
            else:
                success_box(f"Se procesaron **{len(df)}** registros correctamente")
                st.toast(f"{modo} procesado: {len(df)} registros", icon="✅")

                nombre = f"{Path(archivo.name).stem}_procesado.xlsx"
                
                st.download_button(
                    "📥  DESCARGAR RESULTADO",
                    data=to_excel_buffer(df),
                    file_name=nombre,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                
                with st.expander("👀 Vista previa del resultado"):
                    st.dataframe(df.head(15), use_container_width=True)


def tab_brp():
    """Pestaña de distribución BRP."""
    
    with st.expander("📖 ¿Cómo usar esta herramienta?", expanded=False):
        show_tutorial([
            ("Procesa primero", "Ve a la pestaña SEP/PIE y procesa ambos archivos por separado."),
            ("Carga los archivos", "Arrastra los 3 archivos - se detectan automáticamente por nombre."),
            ("Valida (opcional)", "Revisa que los archivos sean del mismo mes."),
            ("Distribuye", "El sistema calcula BRP para SEP, PIE y NORMAL.")
        ])
    
    info_box("Los archivos se detectan por nombre: <b>web*</b> → MINEDUC, <b>sep*</b> → SEP, <b>sn*</b> → PIE/Normal")
    
    st.markdown("---")
    
    # Estado para los archivos
    if 'brp_files' not in st.session_state:
        st.session_state.brp_files = {'web': None, 'sep': None, 'pie': None}
    
    # Uploader múltiple
    st.markdown("##### 📥 Cargar Archivos (arrástralos todos juntos)")
    uploaded_files = st.file_uploader(
        "Arrastra los 3 archivos Excel",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        key="brp_multi_upload"
    )
    
    # Auto-detectar y asignar archivos
    for f in uploaded_files:
        name_lower = f.name.lower()
        if name_lower.startswith('web'):
            st.session_state.brp_files['web'] = f
        elif name_lower.startswith('sep'):
            st.session_state.brp_files['sep'] = f
        elif name_lower.startswith('sn') or 'pie' in name_lower:
            st.session_state.brp_files['pie'] = f
    
    # Mostrar estado de archivos
    st.markdown("##### 📋 Archivos detectados")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📋 MINEDUC (web_sostenedor)**")
        f_web = st.session_state.brp_files['web']
        if f_web:
            st.success(f"✓ {f_web.name}")
        else:
            st.warning("⬜ No detectado (nombre debe empezar con 'web')")
    
    with col2:
        st.markdown("**📊 SEP procesado**")
        f_sep = st.session_state.brp_files['sep']
        if f_sep:
            st.success(f"✓ {f_sep.name}")
        else:
            st.warning("⬜ No detectado (nombre debe empezar con 'sep')")
    
    with col3:
        st.markdown("**📊 PIE/Normal procesado**")
        f_pie = st.session_state.brp_files['pie']
        if f_pie:
            st.success(f"✓ {f_pie.name}")
        else:
            st.warning("⬜ No detectado (nombre debe empezar con 'sn')")
    
    # Botón para limpiar
    if st.button("🔄 Limpiar archivos", key="btn_clear_brp"):
        st.session_state.brp_files = {'web': None, 'sep': None, 'pie': None}
        st.rerun()
    
    # Verificar que están todos
    if not all([f_web, f_sep, f_pie]):
        warning_box("Carga los **3 archivos** para continuar")
        return
    
    # Validación de archivos del mismo mes
    st.markdown("---")
    st.markdown("##### ⚙️ Opciones")
    
    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        solo_validar = st.checkbox("🔍 Solo validar (sin procesar)", value=False, 
                                   help="Revisa los archivos sin generar el resultado final")
    
    # Detectar mes de los archivos por nombre
    meses_detectados = []
    for f, tipo in [(f_sep, 'SEP'), (f_pie, 'PIE')]:
        name = f.name.lower()
        for mes in ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 
                    'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']:
            if mes in name:
                meses_detectados.append((tipo, mes.capitalize()))
                break
    
    if len(meses_detectados) >= 2:
        mes_sep = next((m for t, m in meses_detectados if t == 'SEP'), None)
        mes_pie = next((m for t, m in meses_detectados if t == 'PIE'), None)
        if mes_sep and mes_pie:
            if mes_sep == mes_pie:
                st.success(f"✅ Ambos archivos son de **{mes_sep}**")
            else:
                st.error(f"⚠️ **Meses diferentes:** SEP={mes_sep}, PIE={mes_pie}")
    
    st.markdown("---")
    
    btn_text = "🔍 VALIDAR ARCHIVOS" if solo_validar else "📊 DISTRIBUIR BRP"
    
    if st.button(btn_text, key="btn_brp", use_container_width=True):
        processor = BRPProcessor()
        
        progress = st.progress(0)
        status = st.empty()
        
        def callback(val, msg):
            progress.progress(val / 100)
            status.markdown(f"**⏳ {msg}**")
        
        try:
            # Crear archivos temporales
            paths = []
            for f in [f_web, f_sep, f_pie]:
                tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
                tmp.write(f.getvalue())
                paths.append(Path(tmp.name))
                tmp.close()
            
            out_tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            out_path = Path(out_tmp.name)
            out_tmp.close()
            
            # Procesar
            processor.process_file(
                web_sostenedor_path=paths[0],
                sep_procesado_path=paths[1],
                pie_procesado_path=paths[2],
                output_path=out_path,
                progress_callback=callback
            )
            
            # Mostrar alertas de columnas
            show_column_alerts(processor.get_column_alerts())

            # Leer resultado
            df = pd.read_excel(out_path, sheet_name='BRP_DISTRIBUIDO', engine='openpyxl')
            progress.progress(100)

            # Métricas principales - SEP, PIE, NORMAL
            brp_sep = df['BRP_SEP'].sum() if 'BRP_SEP' in df.columns else 0
            brp_pie = df['BRP_PIE'].sum() if 'BRP_PIE' in df.columns else 0
            brp_normal = df['BRP_NORMAL'].sum() if 'BRP_NORMAL' in df.columns else 0
            total = brp_sep + brp_pie + brp_normal

            if solo_validar:
                status.markdown("**✅ ¡Validación completada!**")
                st.info("📋 Modo validación: revisa los datos antes de procesar definitivamente")
            else:
                status.markdown("**✅ ¡Completado!**")
                success_box("¡Distribución de BRP completada!")
                st.toast(f"BRP distribuido: ${total:,.0f}", icon="💰")

            # Guardar en session_state para sidebar charts
            brp_cols = df.columns
            daem_t = sum(df[f'TOTAL_DAEM_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'TOTAL_DAEM_{s}' in brp_cols)
            cpeip_t = sum(df[f'TOTAL_CPEIP_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL'] if f'TOTAL_CPEIP_{s}' in brp_cols)
            st.session_state['last_brp_result'] = {
                'mes': 'BRP',
                'brp_sep': brp_sep, 'brp_pie': brp_pie,
                'brp_normal': brp_normal, 'brp_total': total,
                'daem_total': daem_t, 'cpeip_total': cpeip_t,
            }

            st.markdown("##### 📈 Resumen de Distribución")

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("SEP", f"${brp_sep:,.0f}")
                if total > 0:
                    st.badge(f"{100*brp_sep/total:.1f}%", color="blue")
            with c2:
                st.metric("PIE", f"${brp_pie:,.0f}")
                if total > 0:
                    st.badge(f"{100*brp_pie/total:.1f}%", color="green")
            with c3:
                st.metric("NORMAL", f"${brp_normal:,.0f}")
                if total > 0:
                    st.badge(f"{100*brp_normal/total:.1f}%", color="orange")
            with c4:
                st.metric("TOTAL", f"${total:,.0f}")
                st.badge("BRP Total", color="violet")

            # Desglose DAEM/CPEIP con totales detallados
            if total > 0:
                show_desglose_daem_cpeip(df)

            # Gráfico de torta
            if total > 0:
                st.markdown("##### 📊 Distribución Visual")

                col_chart1, col_chart2 = st.columns(2)

                with col_chart1:
                    import plotly.express as px

                    df_pie_chart = pd.DataFrame({
                        'Tipo': ['SEP', 'PIE', 'NORMAL'],
                        'Monto': [brp_sep, brp_pie, brp_normal]
                    })

                    fig = px.pie(df_pie_chart, values='Monto', names='Tipo',
                                 title='Distribución por Subvención',
                                 color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b'])
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                with col_chart2:
                    # Detalle reconocimiento vs tramo vs prioritarios
                    recon_total = (df['BRP_RECONOCIMIENTO_SEP'].sum() +
                                   df['BRP_RECONOCIMIENTO_PIE'].sum() +
                                   df['BRP_RECONOCIMIENTO_NORMAL'].sum())
                    tramo_total = (df['BRP_TRAMO_SEP'].sum() +
                                   df['BRP_TRAMO_PIE'].sum() +
                                   df['BRP_TRAMO_NORMAL'].sum())
                    prior_total = (df['CPEIP_PRIOR_SEP'].sum() +
                                   df['CPEIP_PRIOR_PIE'].sum() +
                                   df['CPEIP_PRIOR_NORMAL'].sum()) if 'CPEIP_PRIOR_SEP' in df.columns else 0

                    conceptos = ['Reconocimiento', 'Tramo']
                    montos = [recon_total, tramo_total]
                    colores = ['#8b5cf6', '#ec4899']
                    if prior_total > 0:
                        conceptos.append('Prioritarios')
                        montos.append(prior_total)
                        colores.append('#f97316')

                    df_concepto = pd.DataFrame({
                        'Concepto': conceptos,
                        'Monto': montos
                    })

                    fig2 = px.pie(df_concepto, values='Monto', names='Concepto',
                                  title='Distribución por Concepto',
                                  color_discrete_sequence=colores)
                    fig2.update_traces(textposition='inside', textinfo='percent+label')
                    fig2.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig2, use_container_width=True)

            # Resumen por RBD
            try:
                df_rbd = pd.read_excel(out_path, sheet_name='RESUMEN_POR_RBD', engine='openpyxl')

                with st.expander("🏫 Ver resumen por Establecimiento"):
                    df_rbd_display = add_school_names(df_rbd)
                    st.dataframe(df_rbd_display, use_container_width=True, hide_index=True)

                st.markdown("##### 🏫 Distribución por Escuela")
                show_charts_by_school(df_rbd)
            except:
                pass

            # Multi-Establecimiento
            show_multi_establishment(out_path)

            # Detalle por concepto con tabla cruzada DAEM/CPEIP
            with st.expander("📊 Detalle por concepto (DAEM/CPEIP)"):
                detalle_rows = []
                for subv in ['SEP', 'PIE', 'NORMAL']:
                    daem_r = df[f'DAEM_RECON_{subv}'].sum() if f'DAEM_RECON_{subv}' in df.columns else 0
                    cpeip_r = df[f'CPEIP_RECON_{subv}'].sum() if f'CPEIP_RECON_{subv}' in df.columns else 0
                    detalle_rows.append({
                        'Concepto': 'Reconocimiento', 'Subvención': subv,
                        'DAEM ($)': int(daem_r), 'CPEIP ($)': int(cpeip_r),
                        'Total ($)': int(daem_r + cpeip_r)
                    })
                    daem_t = df[f'DAEM_TRAMO_{subv}'].sum() if f'DAEM_TRAMO_{subv}' in df.columns else 0
                    cpeip_t = df[f'CPEIP_TRAMO_{subv}'].sum() if f'CPEIP_TRAMO_{subv}' in df.columns else 0
                    detalle_rows.append({
                        'Concepto': 'Tramo', 'Subvención': subv,
                        'DAEM ($)': int(daem_t), 'CPEIP ($)': int(cpeip_t),
                        'Total ($)': int(daem_t + cpeip_t)
                    })
                    cpeip_p = df[f'CPEIP_PRIOR_{subv}'].sum() if f'CPEIP_PRIOR_{subv}' in df.columns else 0
                    detalle_rows.append({
                        'Concepto': 'Prioritarios', 'Subvención': subv,
                        'DAEM ($)': 0, 'CPEIP ($)': int(cpeip_p),
                        'Total ($)': int(cpeip_p)
                    })

                df_detalle = pd.DataFrame(detalle_rows)
                st.dataframe(df_detalle, use_container_width=True, hide_index=True)
            
            st.markdown("")
            
            if not solo_validar:
                # Leer archivo completo para descarga
                with open(out_path, 'rb') as f:
                    excel_bytes = f.read()
                
                # Info de hojas
                st.markdown("##### 📁 Archivo generado")
                st.markdown("""
                El archivo contiene **5 hojas**:
                - `BRP_DISTRIBUIDO` → Datos completos con montos MINEDUC originales
                - `RESUMEN_POR_RBD` → Totales por establecimiento
                - `REVISAR` → Casos que requieren revisión
                - `RESUMEN_GENERAL` → Dashboard de resumen
                - `MULTI_ESTABLECIMIENTO` → Desglose de docentes en 2+ escuelas
                """)
                
                # Descarga
                st.download_button(
                    "📥  DESCARGAR ARCHIVO COMPLETO",
                    data=excel_bytes,
                    file_name="brp_distribuido.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            # Verificar si hay hoja de revisión
            try:
                df_revision = pd.read_excel(out_path, sheet_name='REVISAR', engine='openpyxl')

                exceden_44 = df_revision[df_revision['MOTIVO'] == 'EXCEDE 44 HORAS']
                sin_liquidacion = df_revision[df_revision['MOTIVO'] == 'SIN LIQUIDACIÓN']

                st.markdown("---")
                st.markdown("##### ⚠️ Casos para Revisión")

                col1, col2 = st.columns(2)
                with col1:
                    if len(exceden_44) > 0:
                        st.badge(f"{len(exceden_44)} exceden 44hrs", icon="⚠️", color="orange")
                        st.toast(f"{len(exceden_44)} docentes exceden 44 horas", icon="⚠️")
                with col2:
                    if len(sin_liquidacion) > 0:
                        st.badge(f"{len(sin_liquidacion)} sin liquidación", icon="ℹ️", color="blue")

                with st.expander("👀 Ver casos a revisar"):
                    show_revision_table(df_revision)
            except:
                st.success("✅ No hay casos pendientes de revisión")
        
        except Exception as e:
            st.error(f"❌ **Error:** {format_user_error(e)}")
            with st.expander("Ver detalles técnicos"):
                st.code(str(e))


def tab_duplicados():
    """Pestaña de procesamiento de duplicados."""
    
    with st.expander("📖 ¿Cómo usar esta herramienta?", expanded=False):
        show_tutorial([
            ("Carga el archivo principal", "Debe tener una columna llamada DUPLICADOS."),
            ("Carga el complementario", "Archivo adicional para cruzar información."),
            ("Procesa", "El sistema consolida registros duplicados sumando valores.")
        ])
    
    st.markdown("""
    Esta herramienta consolida registros duplicados **sumando valores numéricos** 
    y manteniendo la **primera ocurrencia** de los datos textuales.
    """)
    
    st.markdown("---")
    
    # Archivos
    st.markdown("##### 📥 Archivos de Entrada")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.caption("📄 **Archivo Principal**")
        f1 = st.file_uploader("Principal", type=['xlsx'], key="dup_1", label_visibility="collapsed")
        if f1:
            st.success(f"✓ {f1.name}")
    
    with col2:
        st.caption("📄 **Archivo Complementario**")
        f2 = st.file_uploader("Complementario", type=['xlsx'], key="dup_2", label_visibility="collapsed")
        if f2:
            st.success(f"✓ {f2.name}")
    
    if not all([f1, f2]):
        warning_box("Carga **ambos archivos** para continuar")
        return
    
    st.markdown("")
    
    if st.button("🔄  PROCESAR DUPLICADOS", key="btn_dup", use_container_width=True):
        processor = DuplicadosProcessor()
        df, error = process_files(processor, [f1, f2])
        
        if error:
            st.error(f"❌ **Error:** {error}")
        else:
            success_box(f"Se consolidaron **{len(df)}** registros")
            st.toast(f"Duplicados consolidados: {len(df)} registros", icon="🔄")

            nombre = f"{Path(f1.name).stem}_consolidado.xlsx"
            st.download_button(
                "📥  DESCARGAR RESULTADO",
                data=to_excel_buffer(df),
                file_name=nombre,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


def tab_todo_en_uno():
    """Pestaña de procesamiento integrado Todo en Uno."""

    with st.expander("📖 ¿Cómo usar esta herramienta?", expanded=False):
        show_tutorial([
            ("Carga los 3 archivos BRUTOS", "Arrastra SEP bruto, PIE bruto y web_sostenedor juntos."),
            ("Indica el mes", "Escribe el período en formato YYYY-MM (ej: 2024-01)."),
            ("Compara (opcional)", "Activa comparación si tienes meses anteriores guardados."),
            ("Procesa y descarga", "Obtén Excel procesado e Informe Word con auditoría.")
        ])

    info_box("Carga los <b>3 archivos BRUTOS</b> (SEP, PIE, web_sostenedor) y el sistema procesará todo automáticamente.")

    st.markdown("---")

    # Inicializar repositorio y estado
    repo = BRPRepository()

    if 'todouno_files' not in st.session_state:
        st.session_state.todouno_files = {'sep': None, 'pie': None, 'web': None, 'rem': None}

    # Uploader múltiple
    st.markdown("##### 📥 Cargar Archivos BRUTOS")
    st.caption("Arrastra los 3 archivos: **SEP bruto**, **PIE bruto** y **web_sostenedor**")

    uploaded_files = st.file_uploader(
        "Arrastra los 3 archivos Excel",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        key="todouno_multi_upload"
    )

    # Auto-detectar y asignar archivos
    for f in uploaded_files:
        name_lower = f.name.lower()
        if name_lower.startswith('web'):
            st.session_state.todouno_files['web'] = f
        elif name_lower.startswith('rem') or 'rem' in name_lower:
            st.session_state.todouno_files['rem'] = f
        elif name_lower.startswith('sep') or 'sep' in name_lower:
            st.session_state.todouno_files['sep'] = f
        elif name_lower.startswith('sn') or 'pie' in name_lower or 'normal' in name_lower:
            st.session_state.todouno_files['pie'] = f

    # Upload separado para REM (CSV o Excel)
    st.markdown("")
    st.markdown("##### 📋 Archivo REM (opcional)")
    st.caption("Archivo de remuneraciones para calcular horas disponibles por persona")
    rem_upload = st.file_uploader(
        "Archivo REM (CSV o Excel)",
        type=['csv', 'xlsx', 'xls'],
        key="rem_file_upload"
    )
    if rem_upload:
        st.session_state.todouno_files['rem'] = rem_upload

    # Mostrar estado de archivos detectados
    st.markdown("##### 📋 Archivos detectados")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("**📄 SEP (bruto)**")
        f_sep = st.session_state.todouno_files['sep']
        if f_sep:
            st.success(f"✓ {f_sep.name}")
        else:
            st.warning("⬜ No detectado")

    with col2:
        st.markdown("**📄 PIE/Normal (bruto)**")
        f_pie = st.session_state.todouno_files['pie']
        if f_pie:
            st.success(f"✓ {f_pie.name}")
        else:
            st.warning("⬜ No detectado")

    with col3:
        st.markdown("**📋 MINEDUC (web)**")
        f_web = st.session_state.todouno_files['web']
        if f_web:
            st.success(f"✓ {f_web.name}")
        else:
            st.warning("⬜ No detectado")

    with col4:
        st.markdown("**📋 REM (opcional)**")
        f_rem = st.session_state.todouno_files.get('rem')
        if f_rem:
            st.success(f"✓ {f_rem.name}")
        else:
            st.info("⬜ Sin archivo REM")

    # Botón para limpiar
    if st.button("🔄 Limpiar archivos", key="btn_clear_todouno"):
        st.session_state.todouno_files = {'sep': None, 'pie': None, 'web': None, 'rem': None}
        st.rerun()

    # Verificar que están todos los archivos
    if not all([f_sep, f_pie, f_web]):
        warning_box("Carga los **3 archivos** para continuar")
        return

    st.markdown("---")

    # Configuración del procesamiento
    st.markdown("##### ⚙️ Configuración")

    col1, col2 = st.columns(2)

    with col1:
        # Selector de mes
        mes_default = datetime.now().strftime("%Y-%m")
        mes = st.text_input(
            "📅 Mes de procesamiento",
            value=mes_default,
            help="Formato: YYYY-MM (ej: 2024-01)"
        )

    with col2:
        # Checkbox para comparar
        meses_disponibles = repo.obtener_meses_disponibles()
        comparar = st.checkbox(
            "📊 Comparar con mes anterior",
            value=False,
            disabled=len(meses_disponibles) == 0,
            help="Compara con un procesamiento guardado anteriormente"
        )

        mes_anterior = None
        if comparar and meses_disponibles:
            mes_anterior = st.selectbox(
                "Selecciona mes anterior",
                options=meses_disponibles,
                key="mes_anterior_select"
            )

    # Opción para guardar en BD
    guardar_bd = st.checkbox(
        "💾 Guardar en base de datos (para comparaciones futuras)",
        value=True
    )

    st.markdown("---")

    # Botón de procesar
    if st.button("🚀 PROCESAR TODO", key="btn_todouno", use_container_width=True):
        processor = IntegradoProcessor()

        progress = st.progress(0)
        status = st.empty()

        def callback(val, msg):
            progress.progress(val / 100)
            status.markdown(f"**⏳ {msg}**")

        try:
            # Crear archivos temporales
            paths = {}
            for key, f in [('sep', f_sep), ('pie', f_pie), ('web', f_web)]:
                tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
                tmp.write(f.getvalue())
                paths[key] = Path(tmp.name)
                tmp.close()

            out_tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            out_path = Path(out_tmp.name)
            out_tmp.close()

            # Procesar todo
            df_result, audit = processor.process_all(
                sep_bruto_path=paths['sep'],
                pie_bruto_path=paths['pie'],
                web_sostenedor_path=paths['web'],
                output_path=out_path,
                progress_callback=callback
            )

            progress.progress(100)
            status.markdown("**✅ ¡Procesamiento completado!**")
            st.toast("Procesamiento integrado completado", icon="🎯")

            # Mostrar alertas de columnas
            show_column_alerts(processor.brp_processor.get_column_alerts())

            # Guardar en BD si está marcado
            if guardar_bd:
                try:
                    repo.guardar_procesamiento(mes, df_result)
                    st.success(f"💾 Datos guardados en base de datos para {mes}")
                except Exception as e:
                    st.warning(f"⚠️ No se pudo guardar en BD: {str(e)}")

            # Realizar comparación si está habilitada
            comparacion = None
            if comparar and mes_anterior:
                try:
                    comparador = ComparadorMeses(repo)
                    comparacion = comparador.comparar(mes_anterior, mes)
                except Exception as e:
                    st.warning(f"⚠️ No se pudo realizar comparación: {str(e)}")

            # Mostrar métricas principales
            st.markdown("---")
            st.markdown("##### 📈 Resumen de Distribución")

            brp_sep = df_result['BRP_SEP'].sum() if 'BRP_SEP' in df_result.columns else 0
            brp_pie = df_result['BRP_PIE'].sum() if 'BRP_PIE' in df_result.columns else 0
            brp_normal = df_result['BRP_NORMAL'].sum() if 'BRP_NORMAL' in df_result.columns else 0
            brp_total = brp_sep + brp_pie + brp_normal

            # Calcular DAEM/CPEIP totales para sidebar
            cols = df_result.columns
            daem_total_sidebar = sum(
                df_result[f'TOTAL_DAEM_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL']
                if f'TOTAL_DAEM_{s}' in cols
            )
            cpeip_total_sidebar = sum(
                df_result[f'TOTAL_CPEIP_{s}'].sum() for s in ['SEP', 'PIE', 'NORMAL']
                if f'TOTAL_CPEIP_{s}' in cols
            )

            # Guardar en session_state para sidebar charts
            st.session_state['last_brp_result'] = {
                'mes': mes,
                'brp_sep': brp_sep,
                'brp_pie': brp_pie,
                'brp_normal': brp_normal,
                'brp_total': brp_total,
                'daem_total': daem_total_sidebar,
                'cpeip_total': cpeip_total_sidebar,
            }

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("SEP", f"${brp_sep:,.0f}")
                if brp_total > 0:
                    st.badge(f"{100*brp_sep/brp_total:.1f}%", color="blue")
            with c2:
                st.metric("PIE", f"${brp_pie:,.0f}")
                if brp_total > 0:
                    st.badge(f"{100*brp_pie/brp_total:.1f}%", color="green")
            with c3:
                st.metric("NORMAL", f"${brp_normal:,.0f}")
                if brp_total > 0:
                    st.badge(f"{100*brp_normal/brp_total:.1f}%", color="orange")
            with c4:
                st.metric("TOTAL", f"${brp_total:,.0f}")
                st.badge("BRP Total", color="violet")

            # Desglose DAEM/CPEIP con totales detallados
            if brp_total > 0:
                show_desglose_daem_cpeip(df_result)

            # Gráficos
            if brp_total > 0:
                st.markdown("##### 📊 Distribución Visual")

                col_chart1, col_chart2 = st.columns(2)

                with col_chart1:
                    import plotly.express as px

                    df_pie_chart = pd.DataFrame({
                        'Tipo': ['SEP', 'PIE', 'NORMAL'],
                        'Monto': [brp_sep, brp_pie, brp_normal]
                    })

                    fig = px.pie(df_pie_chart, values='Monto', names='Tipo',
                                 title='Distribución por Subvención',
                                 color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b'])
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                with col_chart2:
                    # Auditoría resumen compacto
                    audit_summary = audit.get_summary()
                    n_err = audit_summary.get('errores', 0)
                    n_warn = audit_summary.get('advertencias', 0)
                    n_total = audit_summary.get('total', 0)
                    st.markdown("**Auditoria**")
                    cols_a = st.columns(3)
                    cols_a[0].metric("Eventos", n_total)
                    cols_a[1].metric("Advertencias", n_warn)
                    cols_a[2].metric("Errores", n_err)

                    # Docentes EIB
                    docentes_eib = len(audit.get_docentes_eib())
                    if docentes_eib > 0:
                        st.warning(f"{docentes_eib} docentes con BRP $0 (posibles EIB)")

            # Mostrar comparación si existe
            if comparacion:
                st.markdown("---")
                st.markdown("##### 📊 Comparación con Mes Anterior")

                resumen = comparacion.get('resumen', {})

                col1, col2, col3 = st.columns(3)
                with col1:
                    diff_doc = resumen.get('docentes_actual', 0) - resumen.get('docentes_anterior', 0)
                    st.metric(
                        "Docentes",
                        f"{resumen.get('docentes_actual', 0):,}",
                        delta=f"{diff_doc:+d}"
                    )
                with col2:
                    st.metric(
                        "Nuevos",
                        f"{resumen.get('docentes_nuevos', 0):,}"
                    )
                with col3:
                    st.metric(
                        "Salieron",
                        f"{resumen.get('docentes_salieron', 0):,}"
                    )

                # BRP
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "BRP Total",
                        f"${resumen.get('brp_actual', 0):,.0f}",
                        delta=f"{resumen.get('cambio_brp_pct', 0):+.1f}%"
                    )
                with col2:
                    cambios_monto = resumen.get('cambios_monto_significativo', 0)
                    if cambios_monto > 0:
                        st.warning(f"⚠️ {cambios_monto} docentes con cambio de monto >10%")

                # Expandir detalles
                with st.expander("👀 Ver detalles de comparación"):
                    # Docentes nuevos
                    nuevos = comparacion.get('docentes_nuevos', [])
                    if nuevos:
                        st.markdown("**Docentes Nuevos:**")
                        df_nuevos = pd.DataFrame(nuevos)
                        st.dataframe(df_nuevos, use_container_width=True)

                    # Docentes que salieron
                    salieron = comparacion.get('docentes_salieron', [])
                    if salieron:
                        st.markdown("**Docentes que Salieron:**")
                        df_salieron = pd.DataFrame(salieron)
                        st.dataframe(df_salieron, use_container_width=True)

                    # Cambios de monto
                    cambios = comparacion.get('cambios_montos', [])
                    if cambios:
                        st.markdown("**Cambios Significativos de Monto:**")
                        df_cambios = pd.DataFrame(cambios)
                        st.dataframe(df_cambios, use_container_width=True)

            # Multi-Establecimiento
            show_multi_establishment(out_path)

            # Log de auditoría detallado
            show_audit_log_detailed(audit)

            st.markdown("---")
            st.markdown("##### 📥 Descargas")

            col1, col2 = st.columns(2)

            with col1:
                # Descargar Excel
                with open(out_path, 'rb') as f:
                    excel_bytes = f.read()

                st.download_button(
                    "📥 DESCARGAR EXCEL",
                    data=excel_bytes,
                    file_name=f"brp_distribuido_{mes}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            with col2:
                # Generar y descargar informe Word
                try:
                    informe = InformeWord()
                    word_buffer = informe.generar(
                        mes=mes,
                        df_resultado=df_result,
                        audit_log=audit,
                        comparacion=comparacion
                    )

                    st.download_button(
                        "📄 DESCARGAR INFORME WORD",
                        data=word_buffer.getvalue(),
                        file_name=f"informe_brp_{mes}.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"Error generando informe Word: {str(e)}")

            # Vista previa
            with st.expander("👀 Vista previa del resultado"):
                st.dataframe(df_result.head(20), use_container_width=True)

            # Procesar REM si se proporcionó
            f_rem = st.session_state.todouno_files.get('rem')
            if f_rem:
                st.markdown("---")
                try:
                    # Crear archivo temporal para REM
                    suffix = '.csv' if f_rem.name.lower().endswith('.csv') else '.xlsx'
                    tmp_rem = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
                    tmp_rem.write(f_rem.getvalue())
                    rem_path = Path(tmp_rem.name)
                    tmp_rem.close()

                    rem_processor = REMProcessor()
                    df_rem_resumen, df_rem_detalle, rem_alertas = rem_processor.process(rem_path)

                    st.toast(f"REM procesado: {len(df_rem_resumen)} personas", icon="📋")
                    show_rem_results(df_rem_resumen, rem_alertas)

                    # Guardar en session_state para sidebar
                    st.session_state['last_rem_result'] = {
                        'personas': len(df_rem_resumen),
                        'total_horas': int(df_rem_resumen['TOTAL'].sum()),
                        'exceden': int(df_rem_resumen['EXCEDE'].sum()),
                    }
                except Exception as e:
                    st.error(f"Error procesando REM: {format_user_error(e)}")
                    with st.expander("Ver detalles técnicos REM"):
                        st.code(str(e))

        except Exception as e:
            st.error(f"❌ **Error:** {format_user_error(e)}")
            import traceback
            with st.expander("Ver detalles técnicos"):
                st.code(traceback.format_exc())


def tab_calculadora():
    """Pestaña de calculadora interactiva BRP."""

    st.markdown("##### 🧮 Calculadora de Distribución BRP")
    st.caption("Simula cómo se distribuye el BRP según las horas en cada tipo de subvención")

    st.markdown("---")

    # Inputs
    col_horas, col_montos = st.columns(2)

    with col_horas:
        st.markdown("**Horas por Subvención**")
        horas_sep = st.number_input("Horas SEP", min_value=0, max_value=44, value=20, step=1, key="calc_sep")
        horas_pie = st.number_input("Horas PIE", min_value=0, max_value=44, value=10, step=1, key="calc_pie")
        horas_sn = st.number_input("Horas Normal (SN)", min_value=0, max_value=44, value=14, step=1, key="calc_sn")
        horas_total = horas_sep + horas_pie + horas_sn

        if horas_total > 44:
            warning_box(f"<b>Total: {horas_total} hrs</b> — Excede el máximo de 44 horas")
        elif horas_total == 0:
            info_box("Ingresa las horas para ver la distribución")
        else:
            st.metric("Total horas", f"{horas_total} hrs")

    with col_montos:
        st.markdown("**Montos MINEDUC (mensuales)**")
        total_reconocimiento = st.number_input(
            "Total Reconocimiento Profesional ($)",
            min_value=0, value=250000, step=10000, key="calc_recon"
        )
        total_tramo = st.number_input(
            "Total Tramo ($)",
            min_value=0, value=180000, step=10000, key="calc_tramo"
        )
        asig_prioritarios = st.number_input(
            "Asignación Alumnos Prioritarios ($)",
            min_value=0, value=30000, step=5000, key="calc_prior"
        )

        # Desglose DAEM/CPEIP (opcional)
        with st.expander("Detalle Subvención/Transferencia (opcional)"):
            subv_recon = st.number_input("Subvención Reconocimiento (DAEM)", min_value=0, value=int(total_reconocimiento * 0.4), step=5000, key="calc_subv_r")
            transf_recon = total_reconocimiento - subv_recon
            st.caption(f"Transferencia Reconocimiento (CPEIP): ${transf_recon:,.0f}")

            subv_tramo = st.number_input("Subvención Tramo (DAEM)", min_value=0, value=int(total_tramo * 0.4), step=5000, key="calc_subv_t")
            transf_tramo = total_tramo - subv_tramo
            st.caption(f"Transferencia Tramo (CPEIP): ${transf_tramo:,.0f}")

    # Cálculo
    if horas_total > 0:
        st.markdown("---")
        st.markdown("##### 📊 Resultado de la Simulación")

        prop_sep = horas_sep / horas_total
        prop_pie = horas_pie / horas_total

        def split3(total_val):
            v_sep = round(total_val * prop_sep)
            v_pie = round(total_val * prop_pie)
            v_sn = total_val - v_sep - v_pie
            return v_sep, v_pie, v_sn

        recon_sep, recon_pie, recon_normal = split3(total_reconocimiento)
        tramo_sep, tramo_pie, tramo_normal = split3(total_tramo)
        prior_sep, prior_pie, prior_normal = split3(asig_prioritarios)

        brp_sep = recon_sep + tramo_sep + prior_sep
        brp_pie = recon_pie + tramo_pie + prior_pie
        brp_normal = recon_normal + tramo_normal + prior_normal
        brp_total = brp_sep + brp_pie + brp_normal

        # Métricas
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("BRP SEP", f"${brp_sep:,.0f}")
            if brp_total > 0:
                st.badge(f"{100*brp_sep/brp_total:.1f}%", color="blue")
        with c2:
            st.metric("BRP PIE", f"${brp_pie:,.0f}")
            if brp_total > 0:
                st.badge(f"{100*brp_pie/brp_total:.1f}%", color="green")
        with c3:
            st.metric("BRP Normal", f"${brp_normal:,.0f}")
            if brp_total > 0:
                st.badge(f"{100*brp_normal/brp_total:.1f}%", color="orange")
        with c4:
            st.metric("BRP TOTAL", f"${brp_total:,.0f}")
            st.badge("Total", color="violet")

        # Tabla detallada
        detalle = pd.DataFrame([
            {'Concepto': 'Reconocimiento', 'SEP ($)': recon_sep, 'PIE ($)': recon_pie, 'Normal ($)': recon_normal, 'Total ($)': total_reconocimiento},
            {'Concepto': 'Tramo', 'SEP ($)': tramo_sep, 'PIE ($)': tramo_pie, 'Normal ($)': tramo_normal, 'Total ($)': total_tramo},
            {'Concepto': 'Prioritarios', 'SEP ($)': prior_sep, 'PIE ($)': prior_pie, 'Normal ($)': prior_normal, 'Total ($)': asig_prioritarios},
            {'Concepto': 'TOTAL', 'SEP ($)': brp_sep, 'PIE ($)': brp_pie, 'Normal ($)': brp_normal, 'Total ($)': brp_total},
        ])
        st.dataframe(detalle, use_container_width=True, hide_index=True)

        # Gráficos
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            import plotly.express as px
            df_pie = pd.DataFrame({
                'Tipo': ['SEP', 'PIE', 'Normal'],
                'Monto': [brp_sep, brp_pie, brp_normal]
            })
            fig = px.pie(df_pie, values='Monto', names='Tipo',
                         title='Distribución por Subvención',
                         color_discrete_sequence=['#3b82f6', '#10b981', '#f59e0b'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_g2:
            # DAEM vs CPEIP
            daem_r_sep, daem_r_pie, daem_r_normal = split3(subv_recon)
            cpeip_r_sep, cpeip_r_pie, cpeip_r_normal = split3(transf_recon)
            daem_t_sep, daem_t_pie, daem_t_normal = split3(subv_tramo)
            cpeip_t_sep, cpeip_t_pie, cpeip_t_normal = split3(transf_tramo)

            total_daem = daem_r_sep + daem_r_pie + daem_r_normal + daem_t_sep + daem_t_pie + daem_t_normal
            total_cpeip = cpeip_r_sep + cpeip_r_pie + cpeip_r_normal + cpeip_t_sep + cpeip_t_pie + cpeip_t_normal + asig_prioritarios

            import plotly.graph_objects as go
            fig2 = go.Figure(data=[go.Bar(
                x=['DAEM', 'CPEIP'],
                y=[total_daem, total_cpeip],
                marker_color=['#6366f1', '#ec4899'],
                text=[f"${total_daem:,.0f}", f"${total_cpeip:,.0f}"],
                textposition='inside',
            )])
            fig2.update_layout(
                title='DAEM vs CPEIP',
                height=300, showlegend=False, yaxis_visible=False,
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Info explicativa
        st.markdown("---")
        st.caption("**Fórmula:** Los montos MINEDUC se distribuyen proporcionalmente según horas. "
                    "Si un docente tiene 20 hrs SEP de 44 totales, el 45.5% de cada concepto va a SEP.")


def tab_ayuda():
    """Pestaña de ayuda."""

    st.markdown("### 🚀 Inicio Rápido")

    st.markdown("""
    RemuPro te ayuda a procesar remuneraciones educativas de forma simple y rápida.

    ---

    ### 📋 Flujo de trabajo recomendado

    **Opción 1 - Todo en Uno (Recomendado):**
    1. Ve a la pestaña **"Todo en Uno"**
    2. Carga los 3 archivos BRUTOS (SEP, PIE, web_sostenedor)
    3. El sistema procesa todo automáticamente
    4. Descarga Excel e Informe Word

    **Opción 2 - Paso a Paso:**
    1. **Procesar SEP** → Pestaña "SEP / PIE", selecciona SEP, sube archivo
    2. **Procesar PIE** → Misma pestaña, selecciona PIE-NORMAL, sube archivo
    3. **Distribuir BRP** → Pestaña "Distribución BRP", carga los 3 archivos

    ---

    ### ❓ Preguntas frecuentes

    **¿Qué hojas debe tener mi archivo?**
    - Para SEP/PIE: `HORAS` y `TOTAL`
    - Para Duplicados: `Hoja1` con columna `DUPLICADOS`

    **¿De dónde saco web_sostenedor?**
    - Se descarga desde la plataforma del MINEDUC

    **¿Qué significa cada columna BRP?**
    - `BRP_SEP`: Monto asignado a Subvención Escolar Preferencial
    - `BRP_PIE`: Monto asignado a Programa de Integración Escolar
    - `BRP_NORMAL`: Monto asignado a Subvención Normal

    **¿Qué significan DAEM y CPEIP?**
    - **DAEM** (Departamento de Administración de Educación Municipal): Corresponde a los montos pagados vía subvención, es decir, lo que paga directamente el municipio
    - **CPEIP** (Centro de Perfeccionamiento, Experimentación e Investigaciones Pedagógicas): Corresponde a los montos por transferencia directa, es decir, lo que transfiere el ministerio
    - La suma de DAEM + CPEIP = BRP Total

    **¿Qué pasa si aparecen alertas de columnas?**
    - Si ve alertas **naranjas** ("Columna no encontrada"): significa que el archivo MINEDUC no tiene una columna esperada y los montos de ese concepto serán $0. Verifique que está usando el archivo correcto y actualizado
    - Si ve alertas **azules** ("Columnas nuevas detectadas"): el MINEDUC agregó columnas nuevas que el sistema no reconoce. No afectan el cálculo pero conviene reportarlas al administrador

    **¿Qué son los docentes EIB?**
    - Docentes del programa de Educación Intercultural Bilingüe
    - Aparecen con BRP $0 en el sistema

    **¿Cómo funciona la comparación de meses?**
    - Guarda el procesamiento en la base de datos
    - Luego puedes comparar con meses anteriores
    - Identifica docentes nuevos, salientes y cambios de monto

    ---

    ### 🎨 Cambiar Tema (Claro/Oscuro)

    1. Haz clic en el menú **⋮** (arriba a la derecha)
    2. Selecciona **Settings**
    3. En **Theme**, elige **Light** o **Dark**

    ---

    ### 🛑 Para cerrar la aplicación

    Cierra la ventana de la terminal o presiona `Ctrl + C`
    """)


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Sidebar
    with st.sidebar:
        st.markdown("### 📊 RemuPro")
        st.caption(f"v{VERSION}")
        st.markdown("---")

        escuelas = load_escuelas()
        st.markdown(f"**🏫 Establecimientos:** {len(escuelas)}")

        # Mostrar lista de escuelas
        with st.expander("Ver escuelas"):
            for rbd, nombre in sorted(escuelas.items(), key=lambda x: x[1]):
                st.markdown(f"**{rbd}** — {nombre}")

        st.markdown("---")
        st.markdown("**Archivos requeridos:**")
        st.markdown("""
        - `web*` → MINEDUC
        - `sep*` → SEP procesado
        - `sn*` / `*pie*` → PIE/Normal
        """)

        st.markdown("---")
        st.caption("DAEM = Subvención (municipio)")
        st.caption("CPEIP = Transferencia (ministerio)")

        # Charts dinámicos del último procesamiento
        st.markdown("---")
        show_sidebar_charts()

    show_header()

    # Tabs principales
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 SEP / PIE",
        "💰 Distribución BRP",
        "🔄 Duplicados",
        "🎯 Todo en Uno",
        "🧮 Calculadora",
        "❓ Ayuda"
    ])

    with tab1:
        tab_sep_pie()

    with tab2:
        tab_brp()

    with tab3:
        tab_duplicados()

    with tab4:
        tab_todo_en_uno()

    with tab5:
        tab_calculadora()

    with tab6:
        tab_ayuda()

    # Footer
    st.markdown(f"""
    <div class="app-footer">
        RemuPro v{VERSION} • © 2026 Eric Aguayo Quintriqueo
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
