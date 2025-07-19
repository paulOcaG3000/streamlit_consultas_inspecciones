import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import pickle
from pathlib import Path
import streamlit_authenticator as stauth
import hashlib

# ===== CONFIGURACIÓN DE AUTENTICACIÓN =====
@st.cache_data(ttl=3600)
def get_user_data():
    """Obtiene datos de usuarios desde Google Sheets"""
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account_sheets"],
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=st.secrets["sheets_ID"],
            range="USERS!A2:C",
        ).execute()
        return result.get("values", [])
    except Exception as e:
        st.error(f"Error al cargar usuarios: {str(e)}")
        return []

# Procesamiento de usuarios
users = get_user_data()
st.write("Datos crudos de Google Sheets:", users)

if not users:
    st.error("No se pudieron cargar los usuarios desde Google Sheets")
    st.stop()
    
names = [user[2] for user in users]
usernames = [user[1] for user in users]
roles = [user[0] for user in users]

# Generación de contraseñas (basadas en DNI + salt)
salt = st.secrets.get("PASSWORD_SALT", "default_salt")
passwords = [hashlib.sha256(f"{dni}{salt}".encode()).hexdigest()[:8] for dni in usernames]

# Configuración del autenticador
authenticator = stauth.Authenticate(
    dict(zip(usernames, [{"name": name, "password": pwd} for name, pwd in zip(names, passwords)])),
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
st.session_state.role = roles[usernames.index(username)]

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
if st.session_state.role in ['ANALISTA', 'SUPERVISOR']:
    search_type = st.radio(
        "Seleccionar tipo de búsqueda:",
        options=["Suministro", "SED"],
        horizontal=True
    )
else:
    search_type = "Suministro"
    st.write("**Búsqueda por Suministro**")


fixed_columns = st.config.get("columns.fixed_columns")
month_columns = st.config.get("columns.month_columns")

all_columns = fixed_columns + month_columns


st.title("Consulta de Consumo por Cliente")
st.markdown("""
Esta aplicación muestra los datos de clientes junto con su consumo histórico 
en los últimos 13 meses.
""")

search_type = st.radio(
    "Seleccionar tipo de búsqueda:",
    options=["Suministro", "SED"],
    horizontal=True
)

if search_type == "Suministro":
    search_value = st.number_input("Ingrese el número de suministro:", step=1 )
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
