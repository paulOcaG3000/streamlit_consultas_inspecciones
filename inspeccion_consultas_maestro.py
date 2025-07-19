import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import streamlit_authenticator as stauth

st.write("Contenido de secrets:", st.secrets)
# ===== CONFIGURACIÓN DE AUTENTICACIÓN =====
# Extraer usuarios desde secrets.toml
def get_users_from_secrets():
    """Obtiene usuarios desde secrets.toml"""
    try:
        # Convertir la estructura de secrets a formato compatible
        credentials = {
            "usernames": {
                username: {
                    "id": user_info["id"],
                    "name": user_info["name"],
                    "password": user_info["password"],
                    "role": user_info["role"]
                }
                for username, user_info in st.secrets["users"].items()
            }
        }
        return credentials
    except Exception as e:
        st.error(f"Error al cargar usuarios: {str(e)}")
        return {"usernames": {}}

# Obtener configuración de usuarios
credentials = get_users_from_secrets()

# Configuración del autenticador
authenticator = stauth.Authenticate(
    credentials["usernames"],
    st.secrets["cookie"]["name"],
    st.secrets["cookie"]["key"],
    st.secrets["cookie"]["expiry_days"],
)

# ===== INTERFAZ DE AUTENTICACIÓN =====
name, authentication_status, username = authenticator.login("Inicio de Sesión", "main")

if not authentication_status:
    st.stop()

if authentication_status is False:
    st.error("Usuario/contraseña incorrectos")
    st.stop()

# ===== POST-LOGIN (APLICACIÓN PRINCIPAL) =====
authenticator.logout("Cerrar Sesión", "sidebar")

# Obtener rol del usuario
st.session_state.role = credentials["usernames"][username]["role"]

# Mostrar información de usuario
st.sidebar.write(f"**Usuario:** {name}")
st.sidebar.write(f"**Rol:** {st.session_state.role.upper()}")

# ===== CONFIGURACIÓN BIGQUERY =====
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account_bigquery"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials)

client = get_bigquery_client()

# ===== INTERFAZ DE LA APLICACIÓN =====
st.title("⚡ Consultas del Maestro ⚡")
st.markdown("""
Esta aplicación muestra los datos de clientes junto con su consumo histórico 
en los últimos 13 meses.
""")

# Control de acceso basado en roles
if st.session_state.role in ['analista', 'supervisor']:
    search_type = st.radio(
        "Seleccionar tipo de búsqueda:",
        options=["Suministro", "SED"],
        horizontal=True
    )
else:
    search_type = "Suministro"
    st.write("**Búsqueda por Suministro**")

# (El resto de tu código de consultas permanece igual...)
fixed_columns = st.config.get("columns.fixed_columns")
month_columns = st.config.get("columns.month_columns")
all_columns = fixed_columns + month_columns

# ... (mantén el resto de tu código de consultas igual)
