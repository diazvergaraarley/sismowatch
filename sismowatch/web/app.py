"""
Interfaz Visual e Interactiva - SismoWatch
Sprint: 6 - Autenticación JWT, Control de Acceso y Dashboard
"""
import os
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# Configuración inicial de la página
st.set_page_config(
    page_title="SismoWatch",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Lectura de la URL base del microservicio API
API_URL = os.getenv("API_URL", "http://api:8000").rstrip("/")

# Inicialización de estado de sesión para autenticación
if "token" not in st.session_state:
    st.session_state.token = None
if "username" not in st.session_state:
    st.session_state.username = None
if "role" not in st.session_state:
    st.session_state.role = None


# ==========================================
# Panel Lateral (Sidebar): Formulario de Autenticación
# ==========================================
with st.sidebar:
    st.markdown("## 🔐 Control de Acceso")

    if not st.session_state.token:
        st.info("Ingresa tus credenciales para acceder a la información sísmica.")
        with st.form("login_form"):
            username_input = st.text_input("Usuario")
            password_input = st.text_input("Contraseña", type="password")
            submitted = st.form_submit_button("Iniciar Sesión", use_container_width=True)

            if submitted:
                if not username_input or not password_input:
                    st.warning("Por favor completa ambos campos.")
                else:
                    try:
                        login_resp = requests.post(
                            f"{API_URL}/auth/login",
                            json={"username": username_input.strip(), "password": password_input.strip()},
                            timeout=5,
                        )
                        if login_resp.status_code == 200:
                            auth_data = login_resp.json()
                            st.session_state.token = auth_data.get("access_token")
                            st.session_state.username = auth_data.get("username")
                            st.session_state.role = auth_data.get("role")
                            st.success(f"Bienvenido, {st.session_state.username} ({st.session_state.role})")
                            st.rerun()
                        elif login_resp.status_code == 401:
                            st.error("Credenciales incorrectas. Verifica usuario o contraseña.")
                        else:
                            st.error(f"Error en autenticación (HTTP {login_resp.status_code})")
                    except requests.exceptions.RequestException as exc:
                        st.error(f"Error de conexión con el servicio de autenticación: {exc}")
    else:
        st.success(f"Sesión activa: **{st.session_state.username}**")
        st.markdown(f"**Rol asignado:** `{st.session_state.role.upper()}`")
        if st.button("Cerrar Sesión", use_container_width=True):
            st.session_state.token = None
            st.session_state.username = None
            st.session_state.role = None
            st.rerun()


# ==========================================
# Pantalla de Bienvenida (Sin Sesión Iniciada)
# ==========================================
if not st.session_state.token:
    st.title("🌍 Bienvenido a SismoWatch")
    st.caption("Plataforma Segura de Monitoreo Sísmico Global con Arquitectura Medallón e Inteligencia Artificial")
    
    st.warning("🔒 Acceso Restringido: Debes iniciar sesión desde el panel lateral para visualizar el Dashboard y el Asistente IA.")

    st.markdown("""
    ---
    ### Sobre la Plataforma
    **SismoWatch** procesa y audita eventos sísmicos en tiempo casi real mediante flujos orquestados en Apache Airflow, 
    transformación con PySpark y almacenamiento analítico en PostgreSQL:

    * 📊 **Dashboard Analítico:** Monitoreo en vivo de magnitud máxima, promedios y distribución geográfica.
    * 🤖 **Agente Inteligente:** Consultas analíticas en lenguaje natural con generación controlada de SQL y verificación estricta.
    * 🛡️ **Seguridad RBAC:** Control de acceso basado en roles (`admin` y `viewer`) mediante tokens criptográficos JWT.
    """)
    st.stop()


# ==========================================
# Sesión Activa: Dashboard y Asistente IA
# ==========================================
st.title("🌍 SismoWatch - Plataforma de Monitoreo Sísmico Global")
st.caption(f"Usuario: **{st.session_state.username}** | Rol: `{st.session_state.role}` | Conexión Segura JWT")

# Headers autorizados para todas las llamadas a la API
auth_headers = {"Authorization": f"Bearer {st.session_state.token}"}

tab_dashboard, tab_chat = st.tabs(["📊 Dashboard en Tiempo Real", "🤖 Asistente de Datos IA"])


# ------------------------------------------
# Pestaña 1: Dashboard en Tiempo Real
# ------------------------------------------
with tab_dashboard:
    st.subheader("Indicadores Clave de Actividad (Últimas 24 Horas)")

    # 1. Petición de KPIs con Token JWT
    kpis = None
    try:
        kpi_response = requests.get(f"{API_URL}/api/kpis", headers=auth_headers, timeout=5)
        if kpi_response.status_code == 200:
            kpis = kpi_response.json()
        elif kpi_response.status_code in [401, 403]:
            st.error("Tu sesión ha expirado o no tienes permisos suficientes. Por favor, vuelve a iniciar sesión.")
        else:
            st.warning(f"No fue posible cargar los KPIs (HTTP {kpi_response.status_code})")
    except requests.exceptions.RequestException as err:
        st.error(f"Error de conexión con la API al consultar KPIs: {err}")

    # Renderizado de métricas
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="Total Eventos Registrados",
            value=kpis.get("total_events", 0) if kpis else "N/D",
        )
    with col2:
        max_mag = kpis.get("max_magnitude", 0.0) if kpis else 0.0
        st.metric(
            label="Magnitud Máxima",
            value=f"{max_mag:.1f} M" if kpis else "N/D",
        )
    with col3:
        avg_mag = kpis.get("avg_magnitude", 0.0) if kpis else 0.0
        st.metric(
            label="Promedio de Magnitud",
            value=f"{avg_mag:.2f} M" if kpis else "N/D",
        )

    st.divider()

    # 2. Petición de Eventos con Token JWT
    events_data = []
    try:
        events_response = requests.get(f"{API_URL}/api/events?limit=50", headers=auth_headers, timeout=5)
        if events_response.status_code == 200:
            events_data = events_response.json().get("events", [])
        else:
            st.warning(f"No fue posible cargar los eventos sísmicos (HTTP {events_response.status_code})")
    except requests.exceptions.RequestException as err:
        st.error(f"Error de conexión con la API al consultar eventos: {err}")

    df_events = pd.DataFrame(events_data)

    if not df_events.empty:
        df_events["time"] = pd.to_datetime(df_events["time"])
        df_events["latitude"] = pd.to_numeric(df_events["latitude"], errors="coerce")
        df_events["longitude"] = pd.to_numeric(df_events["longitude"], errors="coerce")
        df_events["mag"] = pd.to_numeric(df_events["mag"], errors="coerce")
        df_events["depth"] = pd.to_numeric(df_events["depth"], errors="coerce")

        col_map, col_chart = st.columns(2)

        with col_map:
            st.markdown("#### 📍 Ubicación Geográfica de Sismos Recientes")
            map_clean_df = df_events.dropna(subset=["latitude", "longitude"])
            if not map_clean_df.empty:
                st.map(map_clean_df, latitude="latitude", longitude="longitude")
            else:
                st.info("No hay coordenadas disponibles para trazar en el mapa.")

        with col_chart:
            st.markdown("#### 📈 Evolución Temporal de Magnitudes")
            fig = px.scatter(
                df_events,
                x="time",
                y="mag",
                size="mag",
                color="mag",
                hover_data=["place", "depth", "sig"],
                labels={"time": "Fecha y Hora (UTC)", "mag": "Magnitud (M)"},
                title="Sismos por Magnitud a lo largo del tiempo",
                color_continuous_scale="Reds",
            )
            fig.update_layout(
                template="plotly_white",
                margin=dict(l=20, r=20, t=40, b=20),
                height=450,
            )
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 Ver tabla detallada de los últimos 50 eventos"):
            st.dataframe(
                df_events[["id", "mag", "place", "time", "depth", "sig", "latitude", "longitude"]],
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No hay eventos sísmicos disponibles en este momento en la capa Curated.")


# ------------------------------------------
# Pestaña 2: Asistente de Datos IA
# ------------------------------------------
with tab_chat:
    st.subheader("🤖 Consultas en Lenguaje Natural con IA")
    st.caption("Interactúa directamente con el catálogo sísmico. El agente genera SQL seguro sobre la capa Curated y redacta respuestas fundamentadas.")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            {
                "role": "assistant",
                "content": f"¡Hola **{st.session_state.username}**! Soy tu asistente analítico de SismoWatch. Puedes hacerme preguntas en lenguaje natural como:\n"
                           "- *¿Cuál es el sismo de mayor magnitud registrado?*\n"
                           "- *¿Cuántos sismos han ocurrido en las últimas 24 horas?*\n"
                           "- *¿Cuáles son los 5 sismos más recientes y sus ubicaciones?*",
                "sql_query": None,
                "data": None,
            }
        ]

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message.get("sql_query"):
                with st.expander("🔍 Ver detalles técnicos (Transparencia)"):
                    st.markdown("**Consulta SQL ejecutada por el agente:**")
                    st.code(message["sql_query"], language="sql")
                    if message.get("data"):
                        st.markdown("**Datos reales retornados desde PostgreSQL (Capa Curated):**")
                        st.json(message["data"])

    if user_prompt := st.chat_input("Escribe tu pregunta sobre los datos sísmicos..."):
        st.session_state.chat_history.append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando la base de datos con IA..."):
                try:
                    chat_api_response = requests.post(
                        f"{API_URL}/api/chat",
                        headers=auth_headers,
                        json={"question": user_prompt},
                        timeout=30,
                    )

                    if chat_api_response.status_code == 200:
                        payload_res = chat_api_response.json()
                        answer_text = payload_res.get("answer", "")
                        sql_used = payload_res.get("sql_query", "")
                        db_records = payload_res.get("data", [])

                        st.markdown(answer_text)

                        with st.expander("🔍 Ver detalles técnicos (Transparencia)"):
                            st.markdown("**Consulta SQL ejecutada por el agente:**")
                            st.code(sql_used, language="sql")
                            st.markdown("**Datos reales retornados desde PostgreSQL (Capa Curated):**")
                            st.json(db_records)

                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": answer_text,
                            "sql_query": sql_used,
                            "data": db_records,
                        })

                    elif chat_api_response.status_code == 401:
                        st.error("Tu sesión ha expirado o el token es inválido. Por favor, vuelve a iniciar sesión.")
                    elif chat_api_response.status_code == 403:
                        forbidden_detail = chat_api_response.json().get("detail", "Operación no autorizada.")
                        st.error(f"🛡️ Bloqueo de Seguridad (403 Forbidden): {forbidden_detail}")
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": f"⚠️ La consulta fue bloqueada por los filtros de seguridad: {forbidden_detail}",
                            "sql_query": None,
                            "data": None,
                        })
                    else:
                        error_detail = chat_api_response.json().get("detail", chat_api_response.text)
                        st.error(f"Error en el servidor (HTTP {chat_api_response.status_code}): {error_detail}")

                except requests.exceptions.RequestException as exc:
                    st.error(f"Error de conexión con el microservicio API ({API_URL}): {exc}")
