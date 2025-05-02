import pandas as pd
import re # Importar para usar expresiones regulares
import os

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
    # Ajustar el input_csv_path por defecto para usar el archivo generado por el scraper AJAX
    # Si quieres usar el archivo del scraper local, cambia esta línea:
    # input_csv_path = 'data/leads_from_local_file.csv'

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
    # Limpiamos cada número individualmente
    df['Teléfonos Limpios'] = df['Teléfonos'].apply(lambda x: [clean_phone_number(n) for n in str(x).split(',')] if pd.notna(x) and str(x).strip() != 'N/A' else [])
    df['WhatsApp Limpio'] = df['WhatsApp'].apply(lambda x: clean_phone_number(x) if pd.notna(x) and str(x).strip() != 'N/A' else None) # Usar None para facilitar la lógica

    # Consolidar y Deduplicar Teléfonos
    def consolidate_and_deduplicate_phones(row):
        whatsapp = row['WhatsApp Limpio']
        other_phones_list = row['Teléfonos Limpios']

        # Combinar WhatsApp y otros teléfonos en una sola lista (excluyendo N/A y vacíos)
        all_phones = [p for p in other_phones_list if p and p != 'N/A']
        if whatsapp and whatsapp != 'N/A':
             all_phones.insert(0, whatsapp) # Insertar WhatsApp al principio si existe

        # Eliminar duplicados manteniendo el orden (aproximado)
        # Usamos una lista auxiliar para mantener el orden de la primera aparición
        seen = set()
        deduplicated_phones = []
        for phone in all_phones:
            if phone not in seen:
                deduplicated_phones.append(phone)
                seen.add(phone)

        return ', '.join(deduplicated_phones) if deduplicated_phones else 'N/A'

    df['Teléfono'] = df.apply(consolidate_and_deduplicate_phones, axis=1)

    # Eliminamos las columnas temporales y las originales de teléfono/whatsapp
    df = df.drop(columns=['Teléfonos', 'WhatsApp', 'Teléfonos Limpios', 'WhatsApp Limpio'])


    # 2. Eliminar duplicados de filas completas (esto ya estaba y es bueno mantenerlo)
    print(f"Número de filas antes de eliminar duplicados: {len(df)}")
    df.drop_duplicates(inplace=True)
    print(f"Número de filas después de eliminar duplicados: {len(df)}")


    # 3. Limpieza Condicional de Redes Sociales
    # Patrones de los enlaces genéricos de Guía Cores
    GUIA_CORES_FB_PATTERN = r'https://www.facebook.com/sharer/sharer.php\?u=https://www\.guiacores\.com\.ar%2Findex\.php%3Fr%3Dsearch%2Fdetail%26id%3D\d+%26idb%3D\d+'
    GUIA_CORES_IG_PATTERN = r'https://www\.instagram\.com/guiacores/'

    def clean_social_link(link, cores_pattern):
        if pd.isna(link) or str(link).strip() == '':
            return '' # Dejar vacío si ya está vacío o N/A
        link_str = str(link).strip()
        # Comprobar si el enlace coincide con el patrón genérico de Guía Cores
        if re.fullmatch(cores_pattern, link_str):
            return '' # Eliminar si coincide con el patrón genérico
        return link_str # Mantener si es un enlace diferente

    df['Facebook'] = df['Facebook'].apply(lambda x: clean_social_link(x, GUIA_CORES_FB_PATTERN))
    df['Instagram'] = df['Instagram'].apply(lambda x: clean_social_link(x, GUIA_CORES_IG_PATTERN))
    print("Enlaces genéricos de Facebook e Instagram de Guía Cores eliminados.")


    # 4. Eliminar la columna Horario
    if 'Horario' in df.columns:
        df = df.drop(columns=['Horario'])
        print("Columna 'Horario' eliminada.")
    else:
        print("La columna 'Horario' no existe en el CSV.")


    # 5. Establecer Rubros a "ESTUDIO CONTABLE"
    df['Rubros'] = 'Estudio Contable' # Normalizamos la capitalización aquí directamente
    print("Columna 'Rubros' establecida a 'Estudio Contable'.")


    # 6. Normalizar capitalización
    columns_to_normalize = ['Nombre', 'Dirección'] # Rubros ya se normalizó arriba
    for col in columns_to_normalize:
        if col in df.columns:
            df[col] = df[col].apply(normalize_capitalization)
            print(f"Capitalización normalizada para la columna '{col}'.")
        else:
            print(f"Advertencia: La columna '{col}' no existe para normalizar.")


    # Reordenar columnas para que coincidan con el orden solicitado
    # Nombre, Email, Teléfono, Sitio Web, Facebook, Instagram, Rubros, Dirección
    final_columns_order = ['Nombre', 'Email', 'Teléfono', 'Sitio Web', 'Facebook', 'Instagram', 'Rubros', 'Dirección']
    # Filtramos las columnas que realmente existen en el DataFrame
    final_columns_order_existing = [col for col in final_columns_order if col in df.columns]
    # Añadimos cualquier otra columna que pudiera existir y no esté en la lista de orden final (al final)
    other_columns = [col for col in df.columns if col not in final_columns_order_existing]
    df = df[final_columns_order_existing + other_columns]


    # 7. Guardar el CSV limpio
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False, encoding='utf-8')
    print(f"Datos limpios guardados exitosamente en {output_csv_path}")

# Ejecutar el limpiador
if __name__ == "__main__":
    # Por defecto, limpia el archivo generado por el scraper AJAX
    # Si quieres limpiar el archivo del scraper local, cambia la llamada a:
    # clean_leads_csv(input_csv_path='data/leads_from_local_file.csv', output_csv_path='data/leads_from_local_file_cleaned.csv')
    clean_leads_csv()
