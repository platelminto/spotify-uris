import pandas as pd
import os


def process_artist_csv():
    """Read Artist-Genres-URIs.csv, remove first column, save to csvs/6mil/artists.csv"""

    # Input and output file paths
    input_file = "data/6mil-artist-uris/Artist-Genres-URIs.csv"
    output_file = "csvs/6mil/artists.csv"

    # Ensure output directory exists
    os.makedirs("csvs", exist_ok=True)

    print(f"Reading {input_file}...")

    # Read the CSV file
    # Use chunksize for memory efficiency with large files
    chunk_size = 10000
    first_chunk = True

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Remove the first column (index column)
        chunk = chunk.iloc[:, 1:]
        
        # Filter out duplicate header rows
        chunk = chunk[chunk['name'] != 'name']
        
        # Convert JSON array format to PostgreSQL array format
        if 'genres' in chunk.columns:
            import re
            def convert_array(val):
                if pd.isna(val) or val == '[]':
                    return '{}'
                # Remove outer brackets and convert to PostgreSQL format
                val = str(val)
                if val.startswith('[') and val.endswith(']'):
                    # Extract content between brackets
                    content = val[1:-1]
                    # Split by comma and clean up quotes
                    items = re.split(r',\s*', content)
                    cleaned_items = []
                    for item in items:
                        # Remove surrounding quotes (both ' and ")
                        item = item.strip()
                        if (item.startswith("'") and item.endswith("'")) or (item.startswith('"') and item.endswith('"')):
                            item = item[1:-1]
                        cleaned_items.append(item)
                    return '{' + ','.join(cleaned_items) + '}'
                return val
            
            chunk['genres'] = chunk['genres'].apply(convert_array)

        # Write to CSV
        if first_chunk:
            chunk.to_csv(output_file, index=False, mode="w")
            first_chunk = False
        else:
            chunk.to_csv(output_file, index=False, mode="a", header=False)

    print(f"Processed file saved to {output_file}")

    # Print some statistics
    total_rows = sum(1 for _ in pd.read_csv(output_file, chunksize=chunk_size))
    print(f"Total rows processed: {total_rows * chunk_size}")


if __name__ == "__main__":
    process_artist_csv()
