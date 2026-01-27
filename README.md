<div align="center">
  <img src="https://streamlit.io/images/brand/streamlit-logo-primary-light-background-svg.svg" alt="Streamlit Logo" width="400"/>
  <br/><br/>
  <h1 style="border-bottom: none;">📊 RemuPro</h1>
  <p><strong>Sistema Inteligente para el Procesamiento de Remuneraciones Educativas</strong></p>
  <p>Diseñado específicamente para las necesidades del <strong>DAEM de Galvarino</strong>.</p>
</div>

<div align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-blue.svg" alt="Python 3.8+">
  <img src="https://img.shields.io/badge/Framework-Streamlit-red.svg" alt="Framework Streamlit">
  <img src="https://img.shields.io/badge/Licencia-MIT-green.svg" alt="Licencia MIT">
</div>

---

**RemuPro** transforma la complejidad del cálculo de remuneraciones en un proceso simple, rápido y visual. Olvídate de las planillas manuales y los errores; esta herramienta automatiza la distribución de la Bonificación de Reconocimiento Profesional (BRP) y procesa las subvenciones SEP y PIE con precisión milimétrica.

## 🗄️ Integración de Base de Datos para Análisis Histórico

RemuPro integra una base de datos **SQLite** (gestionada con **SQLAlchemy**) para el almacenamiento histórico de los procesamientos mensuales de la BRP. Esto permite:

*   **Registro Detallado:** Guarda un registro de cada procesamiento, incluyendo estadísticas generales (`ProcesamientoMensual`) y los datos individuales de cada docente (`DocenteMensual`).
*   **Análisis Comparativo:** Facilita la comparación de datos entre diferentes meses, identificando tendencias y cambios a lo largo del tiempo.
*   **Trazabilidad:** Proporciona un historial completo de las distribuciones realizadas.

## 📊 Generación de Informes y Visualizaciones Avanzadas

Más allá de los cálculos, RemuPro se destaca por su capacidad de generar informes comprensivos y visualmente ricos:

*   **Informes Excel Multi-Hoja:** Produce un único archivo Excel con hojas dedicadas para diferentes propósitos:
    *   `BRP_DISTRIBUIDO`: Detalle completo de la distribución BRP por cada docente.
    *   `RESUMEN_POR_RBD`: Resumen agregado de los montos BRP por cada establecimiento educacional.
    *   `RESUMEN_GENERAL`: Un dashboard ejecutivo con las cifras clave del procesamiento.
    *   `REVISAR`: Una hoja crucial para las "Alertas Tempranas" que detalla los casos que requieren atención manual.
*   **Informes Word Detallados:** Genera documentos Word que incluyen:
    *   Un resumen ejecutivo claro.
    *   Visualizaciones gráficas (creadas con Matplotlib) para una mejor comprensión de la distribución.
    *   Un registro de auditoría (`AuditLog`) del procesamiento.
    *   Análisis comparativos mensuales para resaltar cambios significativos.
*   **Registro de Auditoría Estructurado (`AuditLog`):** Un sistema interno que registra cada evento, advertencia o error durante el procesamiento, facilitando la depuración y la revisión de cualquier anomalía.

## 🚨 Sistema de Alertas Tempranas Robusto

RemuPro incorpora un sofisticado sistema de alertas para identificar y señalar proactivamente posibles inconsistencias o situaciones que requieren revisión:

*   **Detección de Horas Excesivas:** Alerta automáticamente si algún docente excede el límite legal de 44 horas de trabajo.
*   **Docentes sin Liquidación:** Identifica a los docentes que figuran en el archivo del MINEDUC pero no aparecen en los archivos de liquidación de SEP/PIE.
*   **Cambios Significativos de Monto:** El módulo de comparación mensual detecta variaciones notables (ej. +/- 10%) en los montos BRP de los docentes entre un mes y otro, lo que puede indicar errores o cambios importantes.
*   **Casos en Revisión (Hoja `REVISAR`):** Todos estos casos son compilados en una hoja específica del informe Excel, así como en el informe Word, para una revisión manual eficiente, categorizados por su nivel de importancia.
*   **Flags en Base de Datos:** Las inconsistencias y alertas también se registran en la base de datos (ej. `excede_horas`, `requiere_revision`), permitiendo un seguimiento histórico.

## ✨ Galería: El Poder de los Datos Visuales

RemuPro no solo procesa números, sino que también los convierte en **gráficos interactivos y claros** que facilitan la toma de decisiones. La aplicación genera dashboards dinámicos directamente en la interfaz.

<div align="center">

**Ejemplo de los Dashboards Generados en RemuPro:**
```
      Distribución por Subvención               Distribución por Concepto
┌───────────────────────────────────┐    ┌───────────────────────────────────┐
│        ███████                    │    │      ████████                     │
│    █████████████   SEP (65%)      │    │    █████████████   Reconocimiento │
│  █████████████████                │    │  █████████████████     (75%)      │
│  █████████████████                │    │  █████████████████                │
│    █████████████   PIE (25%)      │    │    █████████████   Tramo (25%)    │
│        ███████                    │    │      ████████                     │
│          ███     NORMAL (10%)     │    │                                   │
└───────────────────────────────────┘    └───────────────────────────────────┘
```
*Los gráficos son interactivos (creados con Plotly) y permiten explorar los datos al pasar el mouse.*

</div>

## 🚀 Características Principales

*   ⚙️ **Distribución BRP Automatizada:** Calcula y distribuye la BRP para las subvenciones SEP, PIE y Normal, aplicando la proporcionalidad correcta para docentes en uno o más establecimientos.
*   🧠 **Procesamiento Inteligente:** Procesa y valida los archivos de liquidación para SEP y PIE, generando un consolidado listo para el siguiente paso.
*   🔍 **Validación de Datos Avanzada:** Detecta automáticamente inconsistencias como docentes que exceden las 44 horas o que figuran en el archivo MINEDUC pero no en las liquidaciones, generando una hoja de `REVISAR` para un fácil seguimiento.
*   📄 **Reportes Completos:** Genera un único archivo Excel con múltiples hojas para un análisis completo:
    *   `BRP_DISTRIBUIDO`: El detalle completo de la distribución por docente.
    *   `RESUMEN_POR_RBD`: Totales agregados por establecimiento.
    *   `RESUMEN_GENERAL`: Un dashboard ejecutivo con las cifras más importantes.
    *   `REVISAR`: Casos que requieren atención manual.
*   🤖 **Auto-detección de Archivos:** Simplemente arrastra los 3 archivos (`web*`, `sep*`, `sn*`) y RemuPro los identificará y asignará automáticamente.
*   🎨 **Interfaz Moderna:** Intuitiva, rápida y con temas claro/oscuro para adaptarse a tu preferencia.

## 📋 Flujo de Trabajo Simplificado

1.  **Procesar Subvenciones:** En la pestaña `SEP / PIE`, procesa y descarga los archivos de subvención SEP y PIE/Normal por separado.
2.  **Distribuir BRP:** En la pestaña `Distribución BRP`, carga los 2 archivos procesados anteriormente junto con el archivo `web_sostenedor` del MINEDUC.
3.  **Analizar y Descargar:** ¡Listo! Revisa los gráficos y métricas, y descarga el completo informe en Excel.

## 🛠️ Instalación y Uso

A continuación se detallan los pasos para instalar y ejecutar RemuPro en macOS y Windows desde cero.

### Para macOS

1.  **Clonar el Repositorio**
    Abre la Terminal y ejecuta el siguiente comando.
    ```bash
    git clone https://github.com/leftra123/remupro_v3.git
    cd remupro_v3
    ```

2.  **Crear y Activar Entorno Virtual**
    Es una buena práctica aislar las dependencias del proyecto.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
    Verás `(venv)` al principio de la línea de comandos, indicando que el entorno está activo.

3.  **Instalar Dependencias**
    Instala todas las librerías necesarias con un solo comando.
    ```bash
    pip install -r requirements.txt
    ```

4.  **Ejecutar RemuPro**
    ¡Ya está todo listo para lanzar la aplicación!
    ```bash
    streamlit run app.py
    ```
    La aplicación se abrirá automáticamente en tu navegador web.

### Para Windows

1.  **Clonar el Repositorio**
    Abre la terminal (CMD o PowerShell) y ejecuta el siguiente comando.
    ```bash
    git clone https://github.com/leftra123/remupro_v3.git
    cd remupro_v3
    ```

2.  **Crear y Activar Entorno Virtual**
    ```bash
    python -m venv venv
    .\\venv\\Scripts\\activate
    ```
    Verás `(venv)` al principio de la línea de comandos.

3.  **Instalar Dependencias**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Ejecutar RemuPro (Opción A: Manual)**
    Lanza la aplicación desde la terminal.
    ```bash
    streamlit run app.py
    ```

5.  **Ejecutar RemuPro (Opción B: Automática)**
    Después de clonar el repositorio, simplemente haz doble clic en el archivo `Iniciar_RemuPro.bat`. Este script instalará las dependencias (la primera vez) y lanzará la aplicación por ti.

## ⚖️ Licencia y Responsabilidad

Este software se distribuye bajo la **Licencia MIT**. Puedes encontrar el texto completo de la licencia en el archivo [LICENSE](LICENSE).

**Importante:** RemuPro es una herramienta de apoyo diseñada para facilitar y agilizar el trabajo del Departamento de Educación. Sin embargo, **la responsabilidad final sobre la veracidad y corrección de los datos procesados recae exclusivamente en el usuario**. Se recomienda encarecidamente **verificar los resultados** generados por la aplicación antes de realizar cualquier pago o informe oficial. El autor no se hace responsable por errores o discrepancias en los cálculos.

---
<div align="center">
  <p>Desarrollado con ❤️ por Eric Aguayo Quintriqueo</p>
</div>