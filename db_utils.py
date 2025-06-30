import os
from io import StringIO
from pandas import read_sql, DataFrame
from sqlalchemy import create_engine, Engine

def get_engine() -> Engine:
    """
    Creates an engine for connecting to the PostgreSQL database using environment variables.
    Returns an SQLAlchemy Engine object.
    """
    db_uri = os.getenv('DB_URI', 'postgresql://gtfs:gtfs@localhost:5432/gtfs_data')
    return create_engine(db_uri, echo=False)

def get_table_cols(engine: Engine, table_name: str) -> list[str]:
    """Retrieves an ordered list of column names for a specified table from the database."""
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """
    try:
        columnNames = read_sql(query, engine, params=(table_name,))['column_name'].tolist()
        return columnNames

    except Exception as e:
        print(f"Error retrieving columns for table {table_name}: {e}")
        raise

def _fetch_data(engine: Engine, query: str) -> DataFrame:
    """Executes an SQL query and returns the result as a DataFrame."""
    try:
        return read_sql(query, engine)
    except Exception as e:
        print(f"Error executing query: {e}")
        raise

def get_stops_data(engine: Engine) -> DataFrame:
    """Fetch stops with valid latitude and longitude from the database."""
    query = """
        SELECT stop_id, stop_name, stop_lat, stop_lon 
        FROM stops 
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """
    return _fetch_data(engine, query)

def get_stop_times_data(engine: Engine) -> DataFrame:
    """Fetch stop times with valid arrival and departure times."""
    query = """
        SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence 
        FROM stop_times
        WHERE arrival_time IS NOT NULL AND departure_time IS NOT NULL
    """
    return _fetch_data(engine, query)

def get_routes_data(engine: Engine) -> DataFrame:
    """Fetch routes data from the database."""
    query = """
        SELECT route_id, route_long_name, route_short_name
        FROM routes
    """
    return _fetch_data(engine, query)

def get_trips_data(engine: Engine) -> DataFrame:
    """Fetch trips data from the database."""
    query = """
        SELECT trip_id, route_id, shape_id, trip_headsign
        FROM trips
    """
    return _fetch_data(engine, query)

def get_shapes_data(engine: Engine) -> DataFrame:
    """Fetch shapes data ordered by sequence."""
    query = """
        SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
        FROM shapes
        ORDER BY shape_pt_sequence
    """
    return _fetch_data(engine, query)

def copy_from_dataframe(engine: Engine, df: DataFrame, table_name: str) -> None:
    """Copies data from a Pandas DataFrame to a PostgreSQL table using the COPY command."""
    try:
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        conn = engine.raw_connection()
        try:
            cur = conn.cursor()
            try:
                cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
            finally:
                cur.close()
            conn.commit()
        finally:
            conn.close()
    
    except Exception as e:
        print(f"Error copying data to table {table_name}: {e}")
        raise