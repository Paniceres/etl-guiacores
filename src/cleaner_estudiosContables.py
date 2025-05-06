import pandas as pd
import re # Importar para usar expresiones regulares
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Database configuration from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'etl_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

def get_db_connection():
    """Establece conexión con la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise

def log_cleaning_session(start_time, end_time, total_processed, total_cleaned, status, error_message=None):
    """Registra una sesión de limpieza en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cleaning_log (
                    start_time, end_time, total_leads_processed,
                    total_leads_cleaned, status, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (start_time, end_time, total_processed, total_cleaned, status, error_message))
            conn.commit()
    except Exception as e:
        print(f"Error al registrar la sesión de limpieza: {e}")
        raise
    finally:
        conn.close()

def save_cleaned_leads_to_db(cleaned_data, source_ids):
    """Guarda los leads limpios en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Preparar los datos para la inserción
            values = []
            for i, lead in enumerate(cleaned_data):
                values.append((
                    lead.get('Nombre', 'N/A'),
                    lead.get('Email', 'N/A'),
                    lead.get('Teléfono', 'N/A'),
                    lead.get('Sitio Web', 'N/A'),
                    lead.get('Facebook', 'N/A'),
                    lead.get('Instagram', 'N/A'),
                    lead.get('Rubros', 'N/A'),
                    lead.get('Dirección', 'N/A'),
                    source_ids[i] if i < len(source_ids) else None
                ))

            # Usar UPSERT para actualizar registros existentes
            execute_values(cur, """
                INSERT INTO leads_clean (
                    nombre, email, telefono, sitio_web, facebook,
                    instagram, rubros, direccion, source_id
                ) VALUES %s
                ON CONFLICT (source_id) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    email = EXCLUDED.email,
                    telefono = EXCLUDED.telefono,
                    sitio_web = EXCLUDED.sitio_web,
                    facebook = EXCLUDED.facebook,
                    instagram = EXCLUDED.instagram,
                    rubros = EXCLUDED.rubros,
                    direccion = EXCLUDED.direccion,
                    updated_at = CURRENT_TIMESTAMP
            """, values)

            conn.commit()
    except Exception as e:
        print(f"Error al guardar leads limpios en la base de datos: {e}")
        raise
    finally:
        conn.close()

def clean_phone_number(phone_str):
    """
    Limpia una cadena de teléfono, dejando solo dígitos y el signo +.
    Elimina espacios en blanco y guiones comunes.
    """
    if pd.isna(phone_str):
        return 'N/A'
    # Convertir a string, eliminar espacios, guiones, paréntesis
    cleaned = re.sub(r'[()\s-]+', '', str(phone_str))
    # Eliminar todo lo que no sea dígito o +
    cleaned = re.sub(r'[^\d+]', '', cleaned)
    return cleaned if cleaned else 'N/A'

def normalize_capitalization(text):
    """Normaliza la capitalización de un texto (primera letra de cada palabra mayúscula, resto minúsculas)."""
    if pd.isna(text) or str(text).strip() == '' or str(text).strip() == 'N/A':
        return 'N/A'
    # Convertir a minúsculas y luego a capitalización de título
    return str(text).lower().title()

def clean_leads_csv(input_csv_path='data/leads_contadores.csv', output_csv_path='data/leads_contadores_cleaned.csv'):
    """
    Lee el CSV de leads, realiza transformaciones y limpieza, y guarda el resultado.
    """
    start_time = datetime.now()
    source_ids = []

    if not os.path.exists(input_csv_path):
        print(f"Error: El archivo de entrada no se encuentra en {input_csv_path}")
        return

    print(f"Leyendo datos de {input_csv_path}...")
    try:
        df = pd.read_csv(input_csv_path)
        print("Datos leídos exitosamente.")
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

    print("Iniciando proceso de limpieza y transformación...")

    # 1. Limpiar y Deduplicar Teléfonos y WhatsApp
    df['Teléfonos Limpios'] = df['Teléfonos'].apply(lambda x: [clean_phone_number(n) for n in str(x).split(',')] if pd.notna(x) and str(x).strip() != 'N/A' else [])
    df['WhatsApp Limpio'] = df['WhatsApp'].apply(lambda x: clean_phone_number(x) if pd.notna(x) and str(x).strip() != 'N/A' else None)

    def consolidate_and_deduplicate_phones(row):
        whatsapp = row['WhatsApp Limpio']
        other_phones_list = row['Teléfonos Limpios']

        all_phones = [p for p in other_phones_list if p and p != 'N/A']
        if whatsapp and whatsapp != 'N/A':
             all_phones.insert(0, whatsapp)

        seen = set()
        deduplicated_phones = []
        for phone in all_phones:
            if phone not in seen:
                deduplicated_phones.append(phone)
                seen.add(phone)

        return ', '.join(deduplicated_phones) if deduplicated_phones else 'N/A'

    df['Teléfono'] = df.apply(consolidate_and_deduplicate_phones, axis=1)
    df = df.drop(columns=['Teléfonos', 'WhatsApp', 'Teléfonos Limpios', 'WhatsApp Limpio'])

    # 2. Eliminar duplicados
    print(f"Número de filas antes de eliminar duplicados: {len(df)}")
    df.drop_duplicates(inplace=True)
    print(f"Número de filas después de eliminar duplicados: {len(df)}")

    # 3. Limpieza de Redes Sociales
    GUIA_CORES_FB_PATTERN = r'https://www.facebook.com/sharer/sharer.php\?u=https://www\.guiacores\.com\.ar%2Findex\.php%3Fr%3Dsearch%2Fdetail%26id%3D\d+%26idb%3D\d+'
    GUIA_CORES_IG_PATTERN = r'https://www\.instagram\.com/guiacores/'

    def clean_social_link(link, cores_pattern):
        if pd.isna(link) or str(link).strip() == '':
            return ''
        link_str = str(link).strip()
        if re.fullmatch(cores_pattern, link_str):
            return ''
        return link_str

    df['Facebook'] = df['Facebook'].apply(lambda x: clean_social_link(x, GUIA_CORES_FB_PATTERN))
    df['Instagram'] = df['Instagram'].apply(lambda x: clean_social_link(x, GUIA_CORES_IG_PATTERN))
    print("Enlaces genéricos de Facebook e Instagram de Guía Cores eliminados.")

    # 4. Eliminar la columna Horario
    if 'Horario' in df.columns:
        df = df.drop(columns=['Horario'])
        print("Columna 'Horario' eliminada.")

    # 5. Establecer Rubros
    df['Rubros'] = 'Estudio Contable'

    # 6. Normalizar capitalización
    columns_to_normalize = ['Nombre', 'Dirección']
    for col in columns_to_normalize:
        if col in df.columns:
            df[col] = df[col].apply(normalize_capitalization)
            print(f"Capitalización normalizada para la columna '{col}'.")

    # Reordenar columnas
    final_columns_order = ['Nombre', 'Email', 'Teléfono', 'Sitio Web', 'Facebook', 'Instagram', 'Rubros', 'Dirección']
    final_columns_order_existing = [col for col in final_columns_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in final_columns_order_existing]
    df = df[final_columns_order_existing + other_columns]

    try:
        # Guardar en la base de datos
        cleaned_data = df.to_dict('records')
        save_cleaned_leads_to_db(cleaned_data, source_ids)
        print("Datos limpios guardados exitosamente en la base de datos")

        # También guardar en CSV como backup
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        df.to_csv(output_csv_path, index=False, encoding='utf-8')
        print(f"Backup CSV guardado en {output_csv_path}")

        # Registrar la sesión de limpieza exitosa
        end_time = datetime.now()
        log_cleaning_session(
            start_time=start_time,
            end_time=end_time,
            total_processed=len(df),
            total_cleaned=len(df),
            status='success'
        )

    except Exception as e:
        # Registrar la sesión de limpieza fallida
        end_time = datetime.now()
        log_cleaning_session(
            start_time=start_time,
            end_time=end_time,
            total_processed=len(df),
            total_cleaned=0,
            status='error',
            error_message=str(e)
        )
        raise

# Ejecutar el limpiador
if __name__ == "__main__":
    clean_leads_csv()
