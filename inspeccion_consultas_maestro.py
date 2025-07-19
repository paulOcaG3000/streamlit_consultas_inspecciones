import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import streamlit_authenticator as stauth
import hashlib

# ===== CONFIGURACIÓN DE AUTENTICACIÓN =====
@st.cache_data(ttl=3600)
def get_user_data():
    """Obtiene usuarios con contraseñas desde Google Sheets"""
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account_sheets"],
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=st.secrets["sheets_ID"],
            range="USERS!A2:D",  # Asegúrate de incluir la columna de contraseña
        ).execute()
        return result.get("values", [])
    except Exception as e:
        st.error(f"Error al cargar usuarios: {str(e)}")
        return []

def setup_authentication():
    users = get_user_data()
    
    if not users:
        st.error("No se encontraron usuarios en la hoja de cálculo")
        st.stop()
    
    credentials = {"usernames": {}}
    roles_mapping = {}
    
    for user in users:
        if len(user) >= 4:  # Verifica las 4 columnas (ROL, DNI, NOMBRE, CONTRASEÑA)
            dni = str(user[1])  # Asegura que DNI sea string
            name = user[2]
            password = user[3]
            role = user[0].lower()  # Normaliza a minúsculas
            
            # Hasheo seguro de la contraseña
            hashed_password = stauth.Hasher([password]).generate()[0]
            
            credentials["usernames"][dni] = {
                "name": name,
                "password": hashed_password
            }
            roles_mapping[dni] = role
    
    return credentials, roles_mapping

# Configuración de autenticación
credentials, roles_mapping = setup_authentication()
authenticator = stauth.Authenticate(
    credentials,
    st.secrets["cookie"]["name"],
    st.secrets["cookie"]["key"],
    st.secrets["cookie"]["expiry_days"],
)

# ===== INTERFAZ DE LOGIN =====
name, authentication_status, username = authenticator.login("Inicio de Sesión", "main")

if not authentication_status:
    st.stop()

if authentication_status is False:
    st.error("Usuario o contraseña incorrectos")
    st.stop()

# ===== APLICACIÓN PRINCIPAL (POST-LOGIN) =====
authenticator.logout("Cerrar Sesión", "sidebar")
st.session_state.role = roles_mapping[username]

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

# ===== CONFIGURACIÓN DE COLUMNAS =====
fixed_columns = st.config.get("columns.fixed_columns")
month_columns = st.config.get("columns.month_columns")

all_columns = fixed_columns + month_columns

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

# Input según el tipo de búsqueda
if search_type == "Suministro":
    search_value = st.number_input("Ingrese el número de suministro:", step=1)
else:
    search_value = st.text_input("Ingrese el código SED:")

def run_query(search_type, search_value):
    """Ejecuta la consulta según el tipo de búsqueda"""
    if not search_value:
        st.warning("Por favor ingrese un valor para buscar")
        return None
    
    columns_str = ", ".join([f'`{col}`' for col in all_columns])
    
    if search_type == "Suministro":
        where_clause = f"WHERE Cuenta = {search_value}"
    else:
        where_clause = f"WHERE SED = '{search_value}'"
    
    query = f"""
        SELECT {columns_str}
        FROM `plenary-cascade-466217-h9.Inspecciones_consultas.maestros_acumulados_actualizados`
        {where_clause}
        LIMIT 10000
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = results.to_dataframe()
        
        if df.empty:
            st.warning("No se encontraron resultados para la búsqueda")
            return None
        
        return df
        
    except Exception as e:
        st.error(f"Error al ejecutar la consulta: {str(e)}")
        return None

if st.button("Obtener datos"):
    if search_value:
        with st.spinner("Cargando datos desde BigQuery..."):
            df = run_query(search_type, search_value)
            
            if df is not None:
                st.success(f"Datos obtenidos correctamente. {len(df)} registros encontrados.")
                
                st.dataframe(df)
                
                st.subheader("Estadísticas de Consumo")
                st.write(df[month_columns].describe())
                
                if search_type == "Suministro" and len(df) == 1:
                    st.subheader("Evolución de Consumo")
                    monthly_data = df[month_columns].transpose()
                    monthly_data.columns = ["Consumo"]
                    st.line_chart(monthly_data)
    else:
        st.warning("Por favor ingrese un valor para buscar")
