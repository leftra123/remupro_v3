"""
RemuPro v2.1 - Sistema de Procesamiento de Remuneraciones Educativas
Interfaz Web con Streamlit
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile

from processors import SEPProcessor, PIEProcessor, DuplicadosProcessor, BRPProcessor

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

VERSION = "2.2.0"

st.set_page_config(
    page_title="RemuPro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
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
    """Muestra una caja de información."""
    st.markdown(f'<div class="info-box">ℹ️ {text}</div>', unsafe_allow_html=True)


def success_box(text: str):
    """Muestra una caja de éxito."""
    st.markdown(f'<div class="success-box">✅ {text}</div>', unsafe_allow_html=True)


def warning_box(text: str):
    """Muestra una caja de advertencia."""
    st.markdown(f'<div class="warning-box">⚠️ {text}</div>', unsafe_allow_html=True)


def card_start(title: str, icon: str = ""):
    """Inicia una tarjeta."""
    st.markdown(f"""
    <div class="custom-card">
        <div class="card-header">{icon} {title}</div>
        <div class="card-content">
    """, unsafe_allow_html=True)


def card_end():
    """Cierra una tarjeta."""
    st.markdown('</div></div>', unsafe_allow_html=True)


def show_tutorial(steps: list):
    """Muestra pasos del tutorial."""
    html = '<div class="tutorial-container">'
    for i, (title, desc) in enumerate(steps, 1):
        html += f'''
        <div class="tutorial-step">
            <div class="step-number">{i}</div>
            <div class="step-content">
                <div class="step-title">{title}</div>
                <div class="step-desc">{desc}</div>
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


def process_files(processor, inputs: list):
    """Procesa archivos con barra de progreso."""
    progress = st.progress(0)
    status = st.empty()
    
    def callback(val, msg):
        progress.progress(val / 100)
        status.markdown(f"**⏳ {msg}**")
    
    try:
        # Crear archivos temporales
        paths = []
        for f in inputs:
            tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            tmp.write(f.getvalue())
            paths.append(Path(tmp.name))
            tmp.close()
        
        out_tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        out_path = Path(out_tmp.name)
        out_tmp.close()
        
        # Ejecutar procesador según cantidad de archivos
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
        status.markdown("**✅ ¡Completado!**")
        return df, None
        
    except Exception as e:
        return None, str(e)


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
            
            # Leer resultado
            df = pd.read_excel(out_path, sheet_name='BRP_DISTRIBUIDO', engine='openpyxl')
            progress.progress(100)
            
            if solo_validar:
                status.markdown("**✅ ¡Validación completada!**")
                st.info("📋 Modo validación: revisa los datos antes de procesar definitivamente")
            else:
                status.markdown("**✅ ¡Completado!**")
                success_box("¡Distribución de BRP completada!")
            
            # Métricas principales - SEP, PIE, NORMAL
            brp_sep = df['BRP_SEP'].sum() if 'BRP_SEP' in df.columns else 0
            brp_pie = df['BRP_PIE'].sum() if 'BRP_PIE' in df.columns else 0
            brp_normal = df['BRP_NORMAL'].sum() if 'BRP_NORMAL' in df.columns else 0
            total = brp_sep + brp_pie + brp_normal
            
            st.markdown("##### 📈 Resumen de Distribución")
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("SEP", f"${brp_sep:,.0f}")
            c2.metric("PIE", f"${brp_pie:,.0f}")
            c3.metric("NORMAL", f"${brp_normal:,.0f}")
            c4.metric("TOTAL", f"${total:,.0f}")
            
            # Gráfico de torta
            if total > 0:
                st.markdown("##### 📊 Distribución Visual")
                
                col_chart1, col_chart2 = st.columns(2)
                
                with col_chart1:
                    # Datos para gráfico
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
                    # Detalle reconocimiento vs tramo
                    recon_total = (df['BRP_RECONOCIMIENTO_SEP'].sum() + 
                                   df['BRP_RECONOCIMIENTO_PIE'].sum() + 
                                   df['BRP_RECONOCIMIENTO_NORMAL'].sum())
                    tramo_total = (df['BRP_TRAMO_SEP'].sum() + 
                                   df['BRP_TRAMO_PIE'].sum() + 
                                   df['BRP_TRAMO_NORMAL'].sum())
                    
                    df_concepto = pd.DataFrame({
                        'Concepto': ['Reconocimiento', 'Tramo'],
                        'Monto': [recon_total, tramo_total]
                    })
                    
                    fig2 = px.pie(df_concepto, values='Monto', names='Concepto',
                                  title='Distribución por Concepto',
                                  color_discrete_sequence=['#8b5cf6', '#ec4899'])
                    fig2.update_traces(textposition='inside', textinfo='percent+label')
                    fig2.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig2, use_container_width=True)
            
            # Resumen por RBD
            with st.expander("🏫 Ver resumen por Establecimiento"):
                try:
                    df_rbd = pd.read_excel(out_path, sheet_name='RESUMEN_POR_RBD', engine='openpyxl')
                    st.dataframe(df_rbd, use_container_width=True)
                except:
                    st.info("No hay resumen por RBD disponible")
            
            # Detalle por concepto
            with st.expander("📊 Detalle por concepto"):
                recon_sep = df['BRP_RECONOCIMIENTO_SEP'].sum() if 'BRP_RECONOCIMIENTO_SEP' in df.columns else 0
                recon_pie = df['BRP_RECONOCIMIENTO_PIE'].sum() if 'BRP_RECONOCIMIENTO_PIE' in df.columns else 0
                recon_normal = df['BRP_RECONOCIMIENTO_NORMAL'].sum() if 'BRP_RECONOCIMIENTO_NORMAL' in df.columns else 0
                
                tramo_sep = df['BRP_TRAMO_SEP'].sum() if 'BRP_TRAMO_SEP' in df.columns else 0
                tramo_pie = df['BRP_TRAMO_PIE'].sum() if 'BRP_TRAMO_PIE' in df.columns else 0
                tramo_normal = df['BRP_TRAMO_NORMAL'].sum() if 'BRP_TRAMO_NORMAL' in df.columns else 0
                
                st.markdown("**Reconocimiento Profesional:**")
                st.write(f"SEP: ${recon_sep:,.0f} | PIE: ${recon_pie:,.0f} | NORMAL: ${recon_normal:,.0f}")
                
                st.markdown("**Tramo:**")
                st.write(f"SEP: ${tramo_sep:,.0f} | PIE: ${tramo_pie:,.0f} | NORMAL: ${tramo_normal:,.0f}")
            
            st.markdown("")
            
            if not solo_validar:
                # Leer archivo completo para descarga
                with open(out_path, 'rb') as f:
                    excel_bytes = f.read()
                
                # Info de hojas
                st.markdown("##### 📁 Archivo generado")
                st.markdown("""
                El archivo contiene **4 hojas**:
                - `BRP_DISTRIBUIDO` → Datos completos con nombres
                - `RESUMEN_POR_RBD` → Totales por establecimiento  
                - `REVISAR` → Casos que requieren revisión
                - `RESUMEN_GENERAL` → Dashboard de resumen
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
                        st.warning(f"**{len(exceden_44)}** docentes exceden 44 horas")
                with col2:
                    if len(sin_liquidacion) > 0:
                        st.info(f"**{len(sin_liquidacion)}** docentes en MINEDUC sin liquidación")
                
                with st.expander("👀 Ver casos a revisar"):
                    st.dataframe(df_revision, use_container_width=True)
            except:
                st.success("✅ No hay casos pendientes de revisión")
        
        except Exception as e:
            st.error(f"❌ **Error:** {str(e)}")


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
            
            nombre = f"{Path(f1.name).stem}_consolidado.xlsx"
            st.download_button(
                "📥  DESCARGAR RESULTADO",
                data=to_excel_buffer(df),
                file_name=nombre,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


def tab_ayuda():
    """Pestaña de ayuda."""
    
    st.markdown("### 🚀 Inicio Rápido")
    
    st.markdown("""
    RemuPro te ayuda a procesar remuneraciones educativas de forma simple y rápida.
    
    ---
    
    ### 📋 Flujo de trabajo recomendado
    
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
    - `BRP_GENERAL`: Monto asignado a Subvención Normal
    
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
    show_header()
    
    # Tabs principales
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 SEP / PIE",
        "💰 Distribución BRP",
        "🔄 Duplicados",
        "❓ Ayuda"
    ])
    
    with tab1:
        tab_sep_pie()
    
    with tab2:
        tab_brp()
    
    with tab3:
        tab_duplicados()
    
    with tab4:
        tab_ayuda()
    
    # Footer
    st.markdown(f"""
    <div class="app-footer">
        RemuPro v{VERSION} • © 2024 Eric Aguayo Quintriqueo
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
