#!/usr/bin/env python

## Import used Libraries
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


## Define constants
## Get the yellow nyc taxi trips data for the given year and month

def run():

    # URL prefix for the data
    prifex = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    year = 2021
    month = 1

    # Postgres connection details
    pg_user = "root"
    pg_password = "root"
    pg_host = "localhost"
    pg_port = 5434
    pg_db = "nyc_taxi"

    # target table parameters
    chunksize = 100000
    target_table = "yellow_taxi_data"
    First = True

    csv_url = f'{prifex}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    ## Create engine to connect to postgres database 
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    
    ## Modify data types
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    ## We don't want to insert all the data at once. We insert in batches and use an iterator for that
    df_iter = pd.read_csv(
        csv_url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )
    
    ## Inserting data chunk by chunk
    for df_chunk in tqdm(df_iter):
        if First:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine, 
                if_exists='replace'
            )
            First = False
        
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
        )
        print(len(df_chunk))

if __name__ == '__main__':
    run()
