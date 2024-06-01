from argparse import ArgumentParser

import requests
from bs4 import BeautifulSoup

import os
from datetime import date, datetime

import pandas as pd
import numpy as np

from exceptions.errors import OperationError
from models import sqls
from models.consts import Category
from models.db import get_db_hook
from models.models import BASE, URIs
from utilities.loggings import MultipurposeLogger
from utilities.utils import load_json_file, find_base_directory

pd.options.display.max_colwidth = 100

URI = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
EXTENSIONS = ['parquet', 'csv', 'json', 'pdf', 'zip']
DATA_DIR = 'data'
# DIM_FILE = 'dim_scrape.csv'

HASH = 'hash'
ADDED_AT = 'add_at'
PATH = 'path'


def dim_scrapper():
    with requests.get(URI) as response:
        try:
            response.raise_for_status()
        except Exception as e:
            raise e

        data = response.text

    links = BeautifulSoup(data, 'html.parser').find_all('a', href=True)

    df = pd.DataFrame({
        'uri': [
            link['href'].strip() if not link['href'].endswith('pdf') else 'https://www.nyc.gov' + link['href'].strip()
            for link in links]
    })

    df = pd.concat(
        [df[df['uri'].str.endswith(ext)] for ext in EXTENSIONS]
    ).reset_index(drop=True).drop_duplicates()

    # enrichemnt
    df[HASH] = pd.util.hash_pandas_object(df).astype(np.int64)

    # enrichemnt - loading processing - TODO: move to the loader function
    df[ADDED_AT] = datetime.now()
    # df[ADDED_AT] = df[ADDED_AT].astype(int)

    df['file'] = df['uri'].str.split('/').str[-1]
    # df.info()

    df['category'] = np.nan
    df.loc[df['uri'].str.endswith('parquet'), 'category'] = df['file'].str.split('_').str[0]

    for cat in Category:
        df.loc[df['category'] == cat.value.lower(), 'category'] = cat.value
    # df.loc[df['category'] == np.nan, 'category'] = Category.OTHER
    # df.loc[df['category'] == cat.value.lower(), 'category'] = Category.GREEN
    # df.loc[df['category'] == Category.FHV.value.lower(), 'category'] = Category.FHV
    # df.loc[df['category'] == Category.FHVHV.value.lower(), 'category'] = Category.FHVHV

    df['date'] = np.nan
    df.loc[df['uri'].str.endswith('parquet'), 'date'] = df['file'].str.split('_').str[-1]
    df.loc[df['uri'].str.endswith('parquet'), 'date'] = df['date'].str.split('.').str[0]
    df['date'] = df['date'].str.replace('-', '')

    df['downloaded'] = False
    return df


def write_to_database(df, conn, table, stream=False):
    old = conn.select(
        query=sqls.SELECT_ALL_FROM_.format(table=table),
    ).set_index([HASH])
    logger.info('-' * 100)

    df = df.set_index([HASH])
    df = df.loc[df.index.difference(old.index)].reset_index().drop_duplicates()

    if not stream:
        conn.insert(
            df=df,
            if_exists='append',
            schema=table.split('.')[0],
            table=table.split('.')[-1],
        )
    else:
        logger.info('-' * 100)
        # TODO: refactor the code, enhance the performance -- insertion becomes in bulks streaming
        for split in np.array_split(df, len(df) // 5000):
            conn.insert(
                df=split,
                if_exists='append',
                schema=table.split('.')[0],
                table=table.split('.')[-1],
            )
        # END TODO
    logger.info('-' * 100)
    pass


def download_file(uri, chunk_size=8192):
    name = uri.split('/')[-1]
    path = os.path.join(DATA_DIR, name)

    try:
        response = requests.get(uri, stream=True)
        response.raise_for_status()  # Check if the request was successful

        with open(path, 'wb') as file:
            # Write the content of the response to the local file in chunks
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)

        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return path
        else:
            raise OperationError("Fail to download the file.")

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        raise Exception(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        raise Exception(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        raise Exception(f"An error occurred: {req_err}")


def retrieve_files_from_dim(type, date, fac, chunk_size=8192):
    # print('-'*150)
    # print(type, date,)
    uris = fac.session.query(URIs).filter(
        URIs.date == int(date),
        URIs.category == type,
        URIs.downloaded == False,
        URIs.path == None,
    ).all()

    if uris:  # has data
        for uri in uris:
            logger.info(f"Going to download : {uri.uri}")
            try:
                path = download_file(uri.uri, chunk_size=chunk_size)
                uri.updated_at = datetime.now()
                uri.downloaded = True
                uri.path = os.path.join(find_base_directory(), path)
            except Exception as e:
                logger.error(f"Failed to download the file due: {e}")

            fac.session.commit()

    else:  # no data
        # logger.info(f"No URIs with the requested type and date {date} and type {type}.")
        logger.info(f"The file is already installed... returning reference for the object")
        uris = fac.session.query(URIs).filter(
            URIs.date == int(date),
            URIs.category == type,
            URIs.downloaded == True,
            URIs.path != None,
        ).all()

    logger.info(f"List of returned URIs is {len(uris)}: {uris}")
    return uris


def read_yellow_file(path):
    df = pd.read_parquet(path)

    columns = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID',
        'DOLocationID', 'payment_type', 'fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'Airport_fee',
    ]

    df = df[columns]

    columns = [
        'vendor_id', 'pep_pick_up_datetime', 'pep_drop_off_datetime', 'passenger_count',
        'trip_distance', 'rate_code_id', 'store_and_fwd_flag', 'pu_location_id',
        'do_location_id', 'payment_type', 'fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'airport_fee',
    ]
    df.columns = columns

    return df


def read_file(uri):  # For more scalable file reading

    if uri.category == Category.YELLOW:
        df = read_yellow_file(uri.path)

    # enrichemnt
    df[HASH] = pd.util.hash_pandas_object(df).astype(np.int64)
    df[ADDED_AT] = datetime.now()
    df[PATH] = uri.path

    return df


def main():
    # logger.initialize_logger_handler(log_level=logging.DEBUG, max_bytes=int(5e+6), backup_count=50)

    config = load_json_file(args.config)
    logger.info(f"Loaded config is: {config}")

    conn, fac = get_db_hook(config.get("database", {}), logger=logger, base=BASE)

    # emailer = MultiPurposeEmailSender.construct_sender_from_dict(data=config.get("email", {}), logger=logger)

    try:
        fac.create_tables()
    except Exception as e:
        logger.error(f"Unable to build the DB for the Application: {e}")
        raise Exception(f"Unable to build the DB for the Application: {e}")

    df = dim_scrapper()

    write_to_database(
        df=df,
        conn=conn,
        table='nyc_taxi_analysis.dim_scrapper'
    )

    del df

    uris = retrieve_files_from_dim(
        type=args.type,
        date=args.date,
        fac=fac
    )


    for uri in uris:
        df = read_file(uri)
        write_to_database(
            df=df,
            conn=conn,
            table='nyc_taxi_analysis.yellow_taxi',
            stream=True
        )

    pass


def cli():
    parser = ArgumentParser()
    parser.add_argument('--config', type=str, required=True, help="The path to the main config .json file.")
    parser.add_argument('--log', type=str, default='logs', help="The path to the log directory.")
    parser.add_argument(
        '--date',
        type=int, default=date.today().strftime("%Y%m"), help="The month of the data to extract [YYYYMM].")
    parser.add_argument(
        '--type',
        type=str.upper,
        default=Category.OTHER.value,
        help=f"The type  of the data to extract {[type.value for type in Category]}.")
    return parser.parse_args()


#  Incremental retrival -- resource can't handle everything at once

if __name__ == "__main__":
    args = cli()
    logger = MultipurposeLogger(name='NYC_TAXI', path=args.log, create=True)

    logger.info(args.type)
    # TODO: validate the correctness of the paths for logs and config
    # TODO: validate the correctness date provided

    main()
    # dim_scrapper()

    # uri = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    # download_file(uri, chunk_size=8192)
