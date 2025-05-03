"""
this script is used to enrich the alzabox data with adddress data from the adresy_cr dataset
this is particularly useful to enrich alzabox data with city_code from adresy_cr dataset
the script reads the cleaned alzabox dataset (data/clean/alzaboxes_cleaned.csv) and the cleaned adresy_cr dataset (data/clean/adresy_cr/combined_adresy_cr_cleaned.csv)
it then merges the two datasets on the street and number columns
the script also adds the city_code from the adresy_cr dataset to the alzabox dataset
"""

import pandas as pd

def enrich_alzabox_data(alzabox_file, adresy_file, output_file):
    # Read the cleaned alzabox dataset
    alzabox_df = pd.read_csv(alzabox_file)

    # Read the cleaned adresy_cr dataset
    addresses_df = pd.read_csv(adresy_file)

    # Select only the relevant columns from the addresses_df dataset
    relevant_columns = ['city_code', 'city','postal_code']
    addresses_df = addresses_df[relevant_columns]
    
    # Apply case insensitive merge
    # alzabox_df['street_lower'] = alzabox_df['street'].str.lower()
    # addresses_df['street_lower'] = addresses_df['street'].str.lower()
    alzabox_df['city_lower'] = alzabox_df['city'].str.lower()
    addresses_df['city_lower'] = addresses_df['city'].str.lower()

    # remove city part number if exists; e.f. "Praha 1" -> "Praha"    
    alzabox_df['city_lower'] = alzabox_df['city_lower'].str.replace(r'\s\d+', '', regex=True)

    # fill in empty values for street_lower
    # alzabox_df['street_lower'] = alzabox_df['street_lower'].fillna('')
    # addresses_df['street_lower'] = addresses_df['street_lower'].fillna('')
    
    # Merge the two datasets on the street and city columns
    # Note: This would produce duplicate rows
    enriched_df = pd.merge(alzabox_df, addresses_df, on=['city_lower', 'postal_code'], how='left')

    # Drop the temporary columns used for merging
    # enriched_df = enriched_df.drop(columns=['street_lower', 'city_lower'])
    enriched_df = enriched_df.drop(columns=['city_lower',])

    # Drop duplicates if any
    enriched_df = enriched_df.drop_duplicates()

    # Add the city_code from the adresy_cr dataset to the alzabox dataset
    # enriched_df['city_code'] = enriched_df['city_code']

    # print number of rows with missing addresses_df and those with matching addresses_df
    missing_addresses = enriched_df[enriched_df['city_code'].isnull()]
    print(f"Number of rows with missing addresses_df: {len(missing_addresses)}")
    print(f"Number of rows with matching addresses_df: {len(enriched_df) - len(missing_addresses)}")
    # print number of rows with missing alzabox_df and those with matching alzabox_df

    # Save the enriched dataset
    enriched_df.to_csv(output_file, index=False)

# Example usage
if __name__ == "__main__":
    alzabox_path = "data/clean/alzaboxes_cleaned.csv"
    adresy_path = "data/clean/adresy_cr/combined_addresses_cz_cleaned.csv"
    output_path = "data/clean/alzaboxes_enriched.csv"
    enrich_alzabox_data(alzabox_path, adresy_path, output_path)