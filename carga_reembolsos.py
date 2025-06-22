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

url = 'https://github.com/rootProyectUG2025/archivosproy/raw/refs/heads/main/CONTROL%20REEMBOLSOS.xlsx'
ruta_excel = 'controlReemb_tmp.xlsx'

response = requests.get(url)

with open(ruta_excel, 'wb') as f:
    f.write(response.content)

# Leer ambas hojas
df_2024 = pd.read_excel(ruta_excel, sheet_name='REEMBOLSOS 2024')
df_2025 = pd.read_excel(ruta_excel, sheet_name='REEMBOLSOS 2025')

# Unir
df_u = pd.concat([df_2024, df_2025], ignore_index=True)

# Renombrar columnas
df_u.columns = df_u.columns.str.lower().str.replace(' ', '_')

# Eliminar filas donde *todas* las columnas están vacías
df_unido = df_u.dropna(how='all')

# LIMPIEZA de 'fecha_envio'
def limpiar_fecha(texto):
    if pd.isna(texto):
        return pd.NaT

    try:
        # Si es string tipo "1/5/22025", corrige el año si es muy largo
        if isinstance(texto, str):
            texto = texto.strip().replace(',', '/') # corrige separadores
            match = re.match(r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4,5})$', texto)
            if match:
                d, m, y = match.groups()
                y = y[-4:] if len(y) > 4 else y  # Corrige 22025 → 2025
                fecha_str = f"{int(d):02d}/{int(m):02d}/{int(y)}"
                return pd.to_datetime(fecha_str, format="%d/%m/%Y", errors="coerce")
        
        # Si ya es datetime
        if isinstance(texto, (pd.Timestamp, datetime)):
            return texto

        # Intenta conversión genérica
        return pd.to_datetime(texto, errors="coerce", dayfirst=True)

    except Exception as e:
        print(f"Error con fecha: {texto} -> {e}")
        return pd.NaT

# Función de limpieza
def limpiar_valor_liquidado(valor):
    if isinstance(valor, str) and 'DEDUCIBLE' in valor.upper():
        return 0
    try:
        return float(valor)
    except:
        return 0

def extraer_observacion(valor):
    if isinstance(valor, str) and 'DEDUCIBLE' in valor.upper():
        return valor
    return ""

# Aplicar limpieza
df_unido['fecha_envío'] = df_unido['fecha_envío'].apply(limpiar_fecha)

# Aplicar limpieza a fecha respuesta
df_unido['fecha_de_respuesta'] = df_unido['fecha_de_respuesta'].apply(limpiar_fecha)

# Aplicar limpieza valor liquidado y observaciones
df_unido['observaciones'] = df_unido['valor_liquidado'].apply(extraer_observacion)
df_unido['valor_liquidado'] = df_unido['valor_liquidado'].apply(limpiar_valor_liquidado)

df_unido['dif_dias'] = (df_unido['fecha_de_respuesta'] - df_unido['fecha_envío']).dt.days

# Corrección de errores de escritura en 'COMPAÑÍA'
df_unido['compañía'] = df_unido['compañía'].replace({
    'MEDIKNE': 'MEDIKEN'
})
df_unido['compañía'] = df_unido['compañía'].str.strip()

# Limpieza de variantes en el campo 'OBSERVACION'
df_unido['observacion'] = df_unido['observacion'].replace({
    'ENVIADO0': 'ENVIADO',
    'enviado': 'ENVIADO',
    'ENVIADOV': 'ENVIADO',
    'ENVIADA': 'ENVIADO'
})

df_unido['observacion'] = df_unido['observacion'].str.strip()

df_unido['agente'] = df_unido['agente'].astype(str).str.strip().str.upper()

df_unido['agente'] = df_unido['agente'].replace({
    'SHIRLEY MUZON': 'SHIRLEY MUÑOZ',
    'RAUUL AVILES': 'RAUL AVILES',
    'NATALIA CHEVEZ': 'NATHALIA CHEVEZ',
    'JOHANNNA MOREIRA': 'JOHANNA MOREIRA',
    'JOHANNA MOREIA': 'JOHANNA MOREIRA',
    'DEYANIRA': 'DEYANIRA PUICON',
    'CHYSTEL CASTRO': 'CRYSTEL CASTRO',
    'CHRYSTEL CASTRO': 'CRYSTEL CASTRO',
    'CHRISTIAN DE PINO': 'CHRISTIAN DEL PINO',
    'CHRISTHIAN DEL PINO': 'CHRISTIAN DEL PINO',
    'CHIRSTIAN DEL PINO': 'CHRISTIAN DEL PINO',
    'ADRINA JARRIN': 'ADRIANA JARRIN',
    'GABY AVILES': 'GABRIELA AVILES'
})

# Guardar en PostgreSQL
df_unido.to_sql('Reembolsos', engine, if_exists='replace', index=False)

print("✅ Datos de Reembolsos limpios y cargados correctamente")

os.remove(ruta_cert)
os.remove(ruta_excel)
