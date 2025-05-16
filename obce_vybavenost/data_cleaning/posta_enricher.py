import time
import pandas as pd
from ruian_client import get_city_code_by_ruian_code

def enrich_posta_data(posta_file, output_file):
    # Process the posta_file in chunks of 50
    chunk_iter = pd.read_csv(posta_file, chunksize=50)
    first_chunk = True
    total_processed = 0
    for chunk in chunk_iter:
        for index, row in chunk.iterrows():
            # process each row
            ruian_code = row.get('ruian_code')
            if pd.notna(ruian_code):
                city_code = get_city_code_by_ruian_code(int(ruian_code))                
                chunk.at[index, 'city_code'] = city_code
                print(f"Found city_code {city_code} for ruian_code {ruian_code}")
            else:
                chunk.at[index, 'city_code'] = None
                print(f"Missing ruian_code for row {index}, setting city_code to None")
            time.sleep(1)
        # Save the chunk to the output file
        if first_chunk:
            chunk.to_csv(output_file, index=False, mode='w')
            first_chunk = False
        else:
            chunk.to_csv(output_file, index=False, mode='a', header=False)
        total_processed += len(chunk)
    print(f"All chunks processed. Total rows: {total_processed}")

# Example usage
if __name__ == "__main__":
    posta_path = "data/clean/posta_cleaned.csv"    
    output_path = "data/clean/posta_enriched.csv"    
    enrich_posta_data(posta_path, output_path)