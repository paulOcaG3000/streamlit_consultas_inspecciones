import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pandas as psd
from googleapiclient.discovery import build

@st.cache_data(ttl=3600)
def get_sheet_data():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId="1vnGFVaAx5e4KzDeCOl26zXMKm2fx4r31BGqu_pi5U4o", 
        range="USERS!A2:C", 
    ).execute()
    return result.get("values", [])

try:
    users = get_sheet_data()
except Exception as e:
    st.error(f"Sheet access failed: {e}")

credentials = service_account.Credentials.from_service_account_file(
    r"C:\MAESTRO\streamlit_consultas_inspecciones\plenary-cascade-466217-h9-4dd26432c9eb.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Columnas fijas que necesitas
fixed_columns = [
    'Cuenta', 'Nombre', 'Direccion', 'Distrito', 'Sector', 
    'Zona', 'Correlativo', 'ActComercial', 'Tarifa', 'Tipo_Acomet',
    'Potencia', 'Estado_Cliente', 'Medidor', 'Fase', 'Factor',
    'FecInstalacion', 'Ultima Lectura Terreno', 
    'Fecha Ultima Lectura Terreno', 'Clave_Ult LectFact',
    'Alimentador', 'SED'
]

# Columnas de meses (las últimas 13 que mencionas)
month_columns = [
    'Jun_24', 'Jul_24', 'Ago_24', 'Set_24', 'Oct_24',
    'Nov_24', 'Dic_24', 'Ene_25', 'Feb_25', 'Mar_25',
    'Abr_25', 'May_25', 'Jun_25'
]

# Combinar todas las columnas
all_columns = fixed_columns + month_columns

# Inicializa el cliente de BigQuery
client = bigquery.Client(credentials=credentials)

# Interfaz de Streamlit
st.title("Análisis de Consumo por Cliente")
st.markdown("""
Esta aplicación muestra los datos de clientes junto con su consumo histórico 
en los últimos 13 meses.
""")

# Widget de selección de tipo de búsqueda
search_type = st.radio(
    "Seleccionar tipo de búsqueda:",
    options=["Suministro", "SED"],
    horizontal=True
)

# Input según el tipo de búsqueda
if search_type == "Suministro":
    search_value = st.number_input("Ingrese el número de suministro:", step=1 )
else:
    search_value = st.text_input("Ingrese el código SED:")

def run_query(search_type, search_value):
    """Ejecuta la consulta según el tipo de búsqueda"""
    if not search_value:
        st.warning("Por favor ingrese un valor para buscar")
        return None
    
    # Construir la consulta SQL
    columns_str = ", ".join([f'`{col}`' for col in all_columns])
    
    if search_type == "Suministro":
        where_clause = f"WHERE Cuenta = {search_value}"
    else:
        where_clause = f"WHERE SED = '{search_value}'"
    
    query = f"""
        SELECT {columns_str}
        FROM `plenary-cascade-466217-h9.Inspecciones_consultas.maestros_acumulados_actualizados`
        {where_clause}
        LIMIT 1000
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

# Botón para ejecutar la consulta
if st.button("Obtener datos"):
    if search_value:
        with st.spinner("Cargando datos desde BigQuery..."):
            df = run_query(search_type, search_value)
            
            if df is not None:
                st.success(f"Datos obtenidos correctamente. {len(df)} registros encontrados.")
                
                # Mostrar dataframe
                st.dataframe(df)
                
                # Mostrar estadísticas de consumo
                st.subheader("Estadísticas de Consumo")
                st.write(df[month_columns].describe())
                
                # Opcional: Gráfico de consumo mensual
                if search_type == "Suministro" and len(df) == 1:
                    st.subheader("Evolución de Consumo")
                    monthly_data = df[month_columns].transpose()
                    monthly_data.columns = ["Consumo"]
                    st.line_chart(monthly_data)
    else:
        st.warning("Por favor ingrese un valor para buscar")