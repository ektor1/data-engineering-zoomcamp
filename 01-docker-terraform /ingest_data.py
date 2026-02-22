#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Ensuring correct data types for Postgres
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


def ingest_data(url: str, engine, target_table: str, chunksize: int) -> None:
    """Read CSV in chunks and insert into Postgres."""
    # Create iterator to add data in chunks in our Postgres db
    df_iter = pd.read_csv(
        url,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
        compression="gzip" if url.endswith('.gz') else None,
        dtype=dtype
    )

    first = True
    for chunk in tqdm(df_iter):
        if first:
            # Create a table in Postgres. If table already exists replace it with this one
            chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False
            print(f"Table {target_table} created")

        # Insert data chunk to db
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )
        print(f"Inserted: {len(chunk)}")

    print(f"Done ingesting data to {target_table}")


@click.command()
@click.option('--pg-user', '-U', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', '-P', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='pgdatabase', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--year', default=2021, show_default=True, type=int)
@click.option('--month', default=1, show_default=True, type=int)
@click.option('--chunksize', default=100000, show_default=True, type=int)
@click.option('--target-table', default='yellow_taxi_data', show_default=True, help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()
