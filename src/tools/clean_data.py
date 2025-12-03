import pandas as pd
import glob
import os
from pathlib import Path
import re

def clean_phones(phone_str):
    if pd.isna(phone_str) or phone_str == 'N/A':
        return 'N/A'
    
    # Split by comma
    phones = [p.strip() for p in str(phone_str).split(',')]
    
    # Deduplicate while preserving order and removing hyphens
    seen = set()
    unique_phones = []
    for p in phones:
        # Remove hyphens and spaces
        clean_p = p.replace('-', '').replace(' ', '')
        
        if clean_p not in seen and clean_p != 'N/A' and clean_p:
            seen.add(clean_p)
            unique_phones.append(clean_p)
            
    if not unique_phones:
        return 'N/A'
        
    return ', '.join(unique_phones)

def clean_social_media(url):
    if pd.isna(url) or url == 'N/A':
        return 'N/A'
    
    url_str = str(url).lower()
    # Check if it's a default guiacores link
    if 'guiacores' in url_str:
        return 'N/A'
        
    return url

def split_address(row):
    direccion_full = row.get('direccion', '')
    if pd.isna(direccion_full) or direccion_full == 'N/A':
        return pd.Series({'direccion': 'N/A', 'localidad': 'N/A'})
    
    # Try to split by " - " which seems to be the separator based on examples
    # Example: "Av. del Libertador 517 - (8318) Plaza Huincul"
    parts = str(direccion_full).split(' - ')
    
    if len(parts) > 1:
        # The last part is likely the locality (possibly with zip code)
        localidad = parts[-1].strip()
        # The rest is the address
        direccion = ' - '.join(parts[:-1]).strip()
        return pd.Series({'direccion': direccion, 'localidad': localidad})
    else:
        return pd.Series({'direccion': direccion_full, 'localidad': 'N/A'})

def main():
    # Define paths
    base_dir = Path(__file__).parent.parent.parent
    processed_dir = base_dir / 'data' / 'processed'
    cleaned_dir = base_dir / 'data' / 'cleaned'
    
    # Create cleaned directory if it doesn't exist
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all CSV files in processed directory
    csv_files = list(processed_dir.glob('*.csv'))
    
    if not csv_files:
        print("No CSV files found in data/processed")
        return

    print(f"Found {len(csv_files)} CSV files to process.")
    
    # Read and concatenate all CSVs
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs:
        print("No data loaded.")
        return
        
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined data shape: {combined_df.shape}")
    
    # --- Transformations ---
    
    # 1. Address Splitting
    print("Splitting addresses...")
    if 'direccion' in combined_df.columns:
        address_split = combined_df.apply(split_address, axis=1)
        combined_df['direccion'] = address_split['direccion']
        combined_df['localidad'] = address_split['localidad']
    
    # 2. Phone Normalization and Deduplication
    print("Cleaning phones...")
    if 'telefonos' in combined_df.columns:
        combined_df['telefonos'] = combined_df['telefonos'].apply(clean_phones)
        
    # 3. Social Media Cleaning
    print("Cleaning social media links...")
    for col in ['facebook', 'instagram']:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_social_media)
            
    # 4. Column Filtering and Reordering
    print("Filtering and reordering columns...")
    
    # Define desired columns in order
    desired_columns = [
        'id_negocio', 
        'nombre', 
        'email', 
        'telefonos', 
        'rubros', 
        'direccion', 
        'localidad', 
        'sitio_web', 
        'facebook', 
        'instagram'
    ]
    
    # Select only columns that exist in the dataframe
    final_columns = [c for c in desired_columns if c in combined_df.columns]
    
    # Create final dataframe
    final_df = combined_df[final_columns]
    
    # Remove duplicates if any (based on id_negocio if it exists, otherwise all columns)
    if 'id_negocio' in final_df.columns:
        final_df = final_df.drop_duplicates(subset=['id_negocio'])
    else:
        final_df = final_df.drop_duplicates()
        
    # Save to CSV
    output_file = cleaned_dir / 'cleaned_data.csv'
    final_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Successfully saved cleaned data to {output_file}")
    print(f"Final data shape: {final_df.shape}")

if __name__ == "__main__":
    main()
