import db_utils
import pandas as pd
import os
import zipfile

class ETL():
    """
    ETL class for processing GTFS data:
    unzip source, extract CSVs, transform DataFrames, and load into PostgreSQL database.
    """
    def __init__(self, source: str, extract_dir: str = 'data/raw') -> None:
        self.source = source
        self.extract_dir = extract_dir
        self.engine = db_utils.get_engine()
    
    def unzip_gtfs(self) -> None:
        """Unzip GTFS source archive to the extraction directory."""
        if not os.path.exists(self.extract_dir):
            os.makedirs(self.extract_dir)
        with zipfile.ZipFile(self.source, 'r') as zip:
            zip.extractall(self.extract_dir)
        print(f"Unzipped {self.source} to {self.extract_dir}")

    def extract(self) -> None:
        """Load GTFS CSV files into pandas DataFrames."""
        # TODO: consider using usecols to load only necessary columns for efficiency.
        self.dfs = {
            'routes': pd.read_csv(os.path.join(self.extract_dir, 'routes.txt')),
            'trips': pd.read_csv(os.path.join(self.extract_dir, 'trips.txt')),
            'stops': pd.read_csv(os.path.join(self.extract_dir, 'stops.txt')),
            'stop_times': pd.read_csv(os.path.join(self.extract_dir, 'stop_times.txt')),
            'shapes': pd.read_csv(os.path.join(self.extract_dir, 'shapes.txt'))
        }

    def transform(self) -> None:
        """Clean and prepare DataFrames for database loading."""
        for table, df in self.dfs.items():
            # Normalize column names to lowercase as per GTFS specification.
            df.columns = [col.lower() for col in df.columns]
            
            # Keep only columns that exist in the database table to avoid errors.
            valid_cols = db_utils.get_table_cols(self.engine, table)
            df = df[[col for col in valid_cols if col in df.columns]].copy()

            # Replace NaN with pd.NA for SQL NULLs compatibility.
            df.fillna(value=pd.NA, inplace=True)
            df.drop_duplicates(inplace=True)
            
            obj_cols = df.select_dtypes(include='object').columns
            df[obj_cols] = df[obj_cols].apply(lambda col: col.str.strip())
            
            self.dfs[table] = df

    def load(self) -> None:
        """Load cleaned DataFrames into their respective database tables."""
        # Processes a snapshot of each DataFrame stored in self.dfs. 
        # After loading, the DataFrame is deleted from memory to free resources.
        for table in list(self.dfs.keys()):
            df = self.dfs[table]
            try:
                if df.empty:
                    print(f"No data to load for {table}. Skipping.")
                    continue
                print(f"Loading {len(df)} rows into {table}")
                db_utils.copy_from_dataframe(self.engine, df, table)
                del self.dfs[table]
            except Exception as e:
                print(f"Failed to load {table}: {e}")
                raise

    def run(self, unzip: bool = True) -> None:
        """Run the entire ETL process: unzip, extract, transform, and load."""
        print("Starting ETL process...")
        if unzip:
            self.unzip_gtfs()
        self.extract()
        self.transform()
        self.load()
        print("ETL process completed.")

if __name__ == "__main__": 
    etl = ETL('data/google_transit.zip')
    etl.run()