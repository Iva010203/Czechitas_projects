import csv
import json

def parse_list_field(field):
    """Split a comma-separated string into a list, stripping whitespace and omitting empty values."""
    if not field or not field.strip():
        return []
    return [item.strip() for item in field.split(',') if item.strip()]

def calculate_decade(year_str):
    """Convert a year string to the first year of its decade, or None if invalid."""
    try:
        year = int(year_str)
        return (year // 10) * 10
    except (ValueError, TypeError):
        return None

def process_row(row):
    """Extract and transform relevant fields from a TSV row."""
    return {
        'title': row['PRIMARYTITLE'],
        'directors': parse_list_field(row['DIRECTOR']),
        'cast': parse_list_field(row['CAST']),
        'genres': parse_list_field(row['GENRES']),
        'decade': calculate_decade(row['STARTYEAR'])
    }


input_file = 'netflix_titles.tsv'
output_file = 'hw02_output.json'
result = []
with open(input_file, encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    for row in reader:
        result.append(process_row(row))
with open(output_file, 'w', encoding='utf-8') as jsonfile:
    json.dump(result, jsonfile, ensure_ascii=False, indent=2)

