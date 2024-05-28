import os
from pprint import pprint
from urllib import request as urequests
from datetime import date

import pandas as pd

pd.options.display.max_colwidth = 100

import requests
from bs4 import BeautifulSoup

HASH = 'hash'
ADDED_AT = 'added_at'

URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DATA_DIR = './data'
DIM_FILE = os.path.join(DATA_DIR, 'scrape_dim.csv')
EXTS = ['csv', "zip", "rar", "parquet", "pdf"]


def dim_scrapper():  # run this once every month

    with requests.get(URL) as response:
        if response.status_code != 200:
            print("Failed to retrieve the web page")
            return []

        raw = response.text
    # print(raw)
    links = BeautifulSoup(
        raw, 'html.parser'
    ).find_all(
        'a', href=True
    )

    df = pd.DataFrame({
        'uri': [
            "https://www.nyc.gov/" + link['href'].strip() if link['href'].strip().endswith('pdf') else link['href'].strip()
            for link in links
        ]
    })

    # Filter DataFrame to include only rows with files ending with the specified extensions
    df = pd.concat([
        df[df['uri'].str.endswith(ext)] for ext in EXTS
    ]).drop_duplicates().reset_index(drop=True)

    # TODO: order here is very important so we don't include the time in the hashing
    df[HASH] = pd.util.hash_pandas_object(df)
    df[ADDED_AT] = int(date.today().strftime('%Y%m%d'))

    # TODO: we should create a dim table and insert the data into it
    #  with more enrichment's [file name, date, extension, is_extracted, number_of_records]
    if not os.path.exists(DIM_FILE):
        df.to_csv(DIM_FILE, index=False)
    else:
        old = pd.read_csv(DIM_FILE)
        df = pd.concat(
            [df, old]
        ).drop_duplicates(
            [HASH]
        ).sort_values(
            [ADDED_AT], ascending=False
        )
        df.to_csv(DIM_FILE, index=False)


def download_files(uri, chunk_size=8192):
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

        return os.path.isfile(path) and os.path.getsize(path) > 0

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        raise Exception(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        raise Exception(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        raise Exception(f"An error occurred: {req_err}")



def get_file_per_date_type(date: int, type: str):
    pass

# dim_scrapper()
# if download_files('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2016-05.parquet'):
#     print("File download successful.")
# else:
#     raise Exception("File download failed: The file does not exist or is empty.")
# pd.read_parquet('./data/yellow_tripdata_2016-05.parquet').info()