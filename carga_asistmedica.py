import requests
import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import datetime
import os

# Leer credenciales desde variables de entorno
usuario = os.getenv('PG_USER')
contraseña = os.getenv('PG_PASSWORD')
host = os.getenv('PG_HOST')
puerto = os.getenv('PG_PORT')
bd = os.getenv('PG_DATABASE')

# Verificación de variables
for var in ['usuario', 'contraseña', 'host', 'puerto', 'bd']:
    if eval(var) is None:
        raise ValueError(f"La variable de entorno '{var.upper()}' no está definida.")

url_cert = 'https://raw.githubusercontent.com/rootProyectUG2025/archivosproy/refs/heads/main/certificadoPostgress.cer'

# Descargar el certificado temporalmente
ruta_cert = 'certificado_temp.cer'
response = requests.get(url_cert)
with open(ruta_cert, 'wb') as f:
    f.write(response.content)

# Conexión a PostgreSQL
engine = create_engine(
    f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{bd}?sslmode=verify-full&sslrootcert={ruta_cert}'
)

url = 'https://github.com/rootProyectUG2025/archivosproy/raw/refs/heads/main/PRODUCCION%20ELITE%20PARTNERS%20-%20KAPPA%20&%20OMEGA%20-%202025.xlsx'
ruta_excel = 'produccionEP_tmp.xlsx'

response = requests.get(url)

with open(ruta_excel, 'wb') as f:
    f.write(response.content)


# Leer Excel
df_asMed = pd.read_excel(ruta_excel, sheet_name='ASIST.MEDICA', header=2)

# Limpieza de columnas vacías y copiar
df_asistMed = df_asMed.iloc[:, 1:].dropna(how='all').copy()

# Normalizar nombres de columnas
df_asistMed.columns = df_asistMed.columns.str.lower().str.replace(' ', '_')

# Función de limpieza de fechas
def limpiar_fecha(texto):
    if pd.isna(texto):
        return pd.NaT

    try:
        if isinstance(texto, str):
            texto = texto.strip().replace(',', '/')
            match = re.match(r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4,5})$', texto)
            if match:
                d, m, y = match.groups()
                y = y[-4:] if len(y) > 4 else y
                fecha_str = f"{int(d):02d}/{int(m):02d}/{int(y)}"
                return pd.to_datetime(fecha_str, format="%d/%m/%Y", errors="coerce")

        if isinstance(texto, (pd.Timestamp, datetime)):
            return texto

        return pd.to_datetime(texto, errors="coerce", dayfirst=True)

    except Exception as e:
        print(f"Error con fecha: {texto} -> {e}")
        return pd.NaT

# Aplicar limpieza de fechas
if 'fecha_1ra_renovac' in df_asistMed.columns:
    df_asistMed['fecha_1ra_renovac'] = df_asistMed['fecha_1ra_renovac'].apply(limpiar_fecha)

df_asistMed['agente'] = df_asistMed['agente'].astype(str).str.strip().str.upper()

df_asistMed['aseguradora'] = df_asistMed['aseguradora'].astype(str).str.strip().str.upper()

df_asistMed['agente'] = df_asistMed['agente'].replace({
    'JULIO DE LUNA': 'JULIO LUNA'
})

df_asistMed['aseguradora'] = df_asistMed['aseguradora'].replace({
    'PLANVITAL': 'PLAN VITAL'
})

# Cargar a PostgreSQL
df_asistMed.to_sql('Asist_Medica', engine, if_exists='replace', index=False)

print("✅ Datos AsistMedicas limpios y cargados correctamente.")

os.remove(ruta_cert)
os.remove(ruta_excel)
