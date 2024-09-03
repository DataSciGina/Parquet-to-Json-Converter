import pandas as pd
import os

def read_parquet(parquet_file):
    """Read the parquet file and returns a DataFrame with all the values."""
    try:
        df = pd.read_parquet(parquet_file)
        return df
    except Exception as e:
        print(f"Error leyendo el archivo Parquet: {e}")
        return None
    
def validate_data(df, null=pd.NA):
    """validate and fill the empty datta with the value of null variable in the DataFrame.
    If null value is empty it will be filled with Pandas' NaN. """
    
    if df.isnull().values.any():
        print("Processing null values...")
        df = df.fillna(null)
    
    return df

def dataframe_to_json(df, output_file, lines=True):
    """Convert DataFrame to JSON. It recives a DataFrame, an output location and a boolean for line format.
    The lines default value is True, it will distribute the records in lines without a coma in the end"""
    try:
        df.to_json(output_file, orient='records', lines=lines)
        print(f"File {output_file} successfully created.")
    except Exception as e:
        print(f"Error exporting data to JSON file: {e}")

def __main__():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_file = os.path.join(script_dir, 'business.parquet')  # replace with the parquet file's
    output_file = os.path.join(script_dir, 'business.json')  # replace with the JSON file's name

    df = read_parquet(parquet_file)

    if df is not None and not df.empty:
        df = validate_data(df)
        dataframe_to_json(df, output_file)

if __name__ == '__main__':
    __main__()
