import streamlit as st
import subprocess
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuración de la página
st.set_page_config(
    page_title="ETL Guía Cores",
    page_icon="🔄",
    layout="wide"
)

# Título y descripción
st.title("ETL Guía Cores")
st.markdown("""
Esta interfaz permite controlar el proceso ETL para extraer datos de Guía Cores.
Puedes elegir entre diferentes modos de operación y configurar los parámetros según tus necesidades.
""")

# Sidebar para selección de modo
st.sidebar.title("Modo de Operación")
mode = st.sidebar.radio(
    "Selecciona el modo de operación:",
    ["Bulk", "Secuencial", "Manual"]
)

# Configuración según el modo seleccionado
if mode == "Bulk":
    st.header("Modo Bulk")
    col1, col2 = st.columns(2)
    with col1:
        start_id = st.number_input("ID Inicial", min_value=1, value=1)
    with col2:
        end_id = st.number_input("ID Final", min_value=start_id, value=1000)
    
    chunk_size = st.slider("Tamaño de Chunk", min_value=10, max_value=1000, value=100)
    num_workers = st.slider("Número de Workers", min_value=1, max_value=8, value=4)

elif mode == "Secuencial":
    st.header("Modo Secuencial")
    rubros = st.text_input("Rubros (separados por coma)", "rubro1,rubro2,rubro3")
    delay = st.slider("Delay entre requests (segundos)", min_value=1, max_value=10, value=2)

else:  # Manual
    st.header("Modo Manual")
    manual_input_type = st.radio("Selecciona el tipo de entrada:", ["URL", "Archivo"])
    if manual_input_type == "URL":
        manual_input_value = st.text_input("URL", "https://www.guiacores.com.ar/index.php?r=search%2Findex")
    else:
        manual_input_value = st.text_input("Ruta del archivo", "path/to/your/file.csv") # Asumiendo un archivo
    output_format = st.selectbox("Formato de salida", ["file", "database", "both"])

# Opciones comunes
st.header("Opciones Generales")
output_dir = st.text_input("Directorio de salida", "data/processed")
log_level = st.selectbox("Nivel de Log", ["INFO", "DEBUG", "WARNING", "ERROR"])

# Botón de ejecución
if st.button("Ejecutar ETL"):
    # Construir comando según el modo
    cmd = ["python", "-m", "src.main"]
    
    if mode == "Bulk":
        cmd.extend([
            "bulk",
            "--start-id", str(start_id),
            "--end-id", str(end_id),
            "--chunk-size", str(chunk_size),
            "--num-workers", str(num_workers)
        ])
    elif mode == "Secuencial":
        cmd.extend([
            "sequential",
            "--rubros", rubros,
            "--delay", str(delay)
        ])
    else:  # Manual
        cmd.extend([
            "manual"
        ])
        if manual_input_type == "URL":
            cmd.extend(["--url", manual_input_value])
        else:
            # Corrected: Pass --file argument for file input
            cmd.extend(["--file", manual_input_value])
        ])
    
    # Agregar opciones comunes
    cmd.extend([
        "--output-dir", output_dir,
        "--log-level", log_level
    ])
    
    # Ejecutar comando
    with st.spinner("Ejecutando ETL..."):
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Mostrar logs en tiempo real
            log_placeholder = st.empty()
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    log_placeholder.text(output.strip())
            
            # Read stderr after the process finishes
            stderr_output = process.stderr.read()

            # Mostrar errores si los hay
            if stderr_output:
                st.error(f"Error durante la ejecución:\n{stderr_output}")

            # Verificar resultado
            if process.returncode == 0:
                st.success("ETL completado exitosamente!")
            else:
                st.error("Error en la ejecución del ETL")
                
        except Exception as e:
            st.error(f"Error al ejecutar el comando: {str(e)}")

# Sección de monitoreo
st.header("Monitoreo")
tab1, tab2 = st.tabs(["Logs", "Estadísticas"])

with tab1:
    # Mostrar últimos logs
    log_file = Path("logs/main.log")
    if log_file.exists():
        with open(log_file, "r") as f:
            logs = f.readlines()[-100:]  # Últimas 100 líneas
        st.text_area("Últimos logs", "".join(logs), height=300)
    else:
        st.warning("No hay logs disponibles")

with tab2:
    # Conectar a la base de datos y mostrar estadísticas
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "guiacores"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres")
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Total de registros
            cur.execute("SELECT COUNT(*) as total FROM businesses")
            total = cur.fetchone()["total"]
            st.metric("Total de negocios", total)
            
            # Registros por día
            cur.execute("""
                SELECT DATE(fecha_extraccion) as fecha, COUNT(*) as cantidad
                FROM businesses
                GROUP BY DATE(fecha_extraccion)
                ORDER BY fecha DESC
                LIMIT 7
            """)
            daily_stats = pd.DataFrame(cur.fetchall())
            if not daily_stats.empty:
                st.line_chart(daily_stats.set_index("fecha")["cantidad"])
            
            # Últimos registros
            cur.execute("""
                SELECT nombre, direccion, fecha_extraccion
                FROM businesses
                ORDER BY fecha_extraccion DESC
                LIMIT 5
            """)
            recent = pd.DataFrame(cur.fetchall())
            if not recent.empty:
                st.dataframe(recent)
                
    except Exception as e:
        st.error(f"Error al conectar con la base de datos: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close() 