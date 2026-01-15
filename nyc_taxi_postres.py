#!/usr/bin/env python

## Import used Libraries
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


@click.command()
@click.option(
    "--prefix",
    default="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow",
    show_default=True,
    help="Base URL prefix for the dataset"
)

@click.option("--year", default=2021, type=int, show_default=True, help="Year of the data")
@click.option("--month", default=1, type=int, show_default=True, help="Month of the data")
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-password", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5434, type=int, show_default=True, help="Postgres port")
@click.option("--pg-db", default="nyc_taxi", show_default=True, help="Postgres database name")
@click.option("--chunksize", default=100000, type=int, show_default=True, help="Number of rows per chunk when loading data")
@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Target table name in Postgres")


def run( prefix, year, month, pg_user, pg_password, pg_host, pg_port, pg_db, chunksize, target_table,):
    
    First = True
    csv_url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
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
