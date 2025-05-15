"""
this script is used to clean the posta dataset
it reads data from the posta raw dataset (data/raw/posta.csv) and cleans the data:
3. if street ends with "nám." or "nám." it will be replaced with "náměstí"
    e.g. "Komenského nám." will be replaced with "Komenského náměstí"
"""

import pandas as pd

def clean_post_office_data(input_file, output_file):
    # Read the raw dataset
    df = pd.read_csv(input_file, sep=';', encoding='windows-1250', skiprows=1)

    # select only the relevant columns and rename them
    relevant_columns = {
        
        'NAZ_PROVOZOVNY': 'name',
        'OBEC': 'city',
        'PSC': 'postal_code',
        'NAZ_ULICE': 'street',
        'CISLO_POP': 'building_number',
        'CISLO_OR': 'orientation_number',
        
    }
    df = df[list(relevant_columns.keys())].rename(columns=relevant_columns)

    # Replace 'nám.' or 'nám.' with 'náměstí' in the street column
    df['street'] = df['street'].str.replace(r'\bnám\.\s*', 'náměstí ', regex=True)

    # Remove leading and trailing spaces
    df['street'] = df['street'].str.strip()    

    
    # Save the cleaned dataset
    df.to_csv(output_file, index=False)

# usage
if __name__ == "__main__":
    input_path = "data/raw/posta.csv"
    output_path = "data/clean/posta_cleaned.csv"
    clean_post_office_data(input_path, output_path)