import streamlit as st
import sys
import os
from pathlib import Path

# Add the parent directory of src to the sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.main import run_bulk_etl, process_manual_input, run_sequential_etl

st.title("ETL Pipeline GUI")
st.write("Select an ETL mode and provide the necessary parameters.")

# ETL Mode Selection
mode = st.radio(
    "Select ETL Mode",
    ("Bulk", "Manual", "Sequential")
)

result = None

if mode == "Bulk":
    st.header("Bulk ETL")
    start_id = st.number_input("Start ID", min_value=0, step=1)
    end_id = st.number_input("End ID", min_value=0, step=1)
    if st.button("Run Bulk ETL"):
        if start_id is not None and end_id is not None:
            result = run_bulk_etl(start_id=start_id, end_id=end_id, output="file")
        else:
            st.warning("Please enter both Start ID and End ID.")

elif mode == "Manual":
    st.header("Manual ETL")
    manual_input_type = st.radio(
        "Select Input Type",
        ("URL", "HTML File Directory")
    )

    manual_url = None
    manual_file_path = None

    if manual_input_type == "URL":
        manual_url = st.text_input("Enter URL")
    elif manual_input_type == "HTML File Directory":
        manual_file_path = st.text_input("Enter path to HTML file directory") # Streamlit doesn't have a directory picker, text input is a workaround

    if st.button("Run Manual ETL"):
        if manual_input_type == "URL" and manual_url:
            result = process_manual_input(url=manual_url, output="file")
        elif manual_input_type == "HTML File Directory" and manual_file_path:
             if Path(manual_file_path).is_dir():
                result = process_manual_input(file=manual_file_path, output="file")
             else:
                st.warning("Please enter a valid directory path.")
        else:
            st.warning("Please provide either a URL or a directory path.")

elif mode == "Sequential":
    st.header("Sequential ETL")
    rubros_input = st.text_input("Enter Rubros (comma-separated)")
    localidades_input = st.text_input("Enter Localidades (comma-separated)")

    rubros_list = [r.strip() for r in rubros_input.split(',') if r.strip()] if rubros_input else None
    localidades_list = [l.strip() for l in localidades_input.split(',') if l.strip()] if localidades_input else None

    if st.button("Run Sequential ETL"):
        if rubros_list or localidades_list:
             progress_bar = st.progress(0)
             status_text = st.empty()
             
             def update_progress(current, total, message):
                 status_text.text(message)
                 if total and total > 0:
                     progress = min(current / total, 1.0)
                     progress_bar.progress(progress)
                 else:
                     progress_bar.progress(0)

             result = run_sequential_etl(rubros=rubros_list, localidades=localidades_list, output="file", progress_callback=update_progress)
             progress_bar.progress(1.0) # Ensure it completes
             status_text.text("Recolección completada.")
        else:
            st.warning("Please enter at least one Rubro or Localidad.")

# Display Result
if result:
    st.subheader("ETL Result")
    st.write(f"Status: {result.get('status', 'Unknown')}")
    st.write(f"Message: {result.get('message', 'No message')}")
    if result.get('records_processed') is not None:
        st.write(f"Records Processed: {result.get('records_processed')}")

# Add a section to display and potentially download generated files
st.subheader("Generated Files (in data/processed)")
output_dir = Path("data/processed")
if output_dir.exists():
    files = list(output_dir.glob("*.csv"))
    if files:
        st.write("Generated CSV files:")
        for file in files:
            st.write(f"- {file.name}")
            # Optional: Add download button for each file (requires reading the file)
            # with open(file, "rb") as f:
            #     st.download_button(
            #         label=f"Download {file.name}",
            #         data=f,
            #         file_name=file.name,
            #         mime="text/csv"
            #     )
    else:
        st.write("No CSV files generated yet.")
else:
    st.write("The data/processed directory does not exist yet.")
