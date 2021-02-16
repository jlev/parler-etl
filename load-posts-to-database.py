import os, glob
import argparse
import orjson, json
import time
import csv
import io
import logging
import datetime

import sqlalchemy
import psycopg2
from tqdm import tqdm

log = logging.getLogger(__name__)


def load_ndjson_file(filename, connection, tablename, pbar):
    with open(filename, 'r') as file:
        cursor = connection.cursor()
        lines_loaded = 0

        for line in file:
            row = transform_row(orjson.loads(line))
            copy_to_database(row, cursor, tablename)
            pbar.update(1)
            connection.commit()
            lines_loaded += 1

        return lines_loaded

def transform_row(row):
    # don't create sqlalchemy models here, just match the defined format and order
    return [
        row['id'], 
        row.get('username',''),
        row.get('creator',''),
        datetime.datetime.strptime(row['createdAt'],"%Y%m%d%H%M%S") if 'createdAt' in row else None,
        row.get('body',''),
        row.get('impressions',0),
        row.get('comments',0),
        row.get('upvotes',0),
        json.dumps(row['links']) if row['links'] else None # maintain json string
    ]

def copy_to_database(row, cursor, tablename):
    # use raw cursor copy for speed
    
    # setup buffer
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
    writer.writerow(row)

    # reset buffer
    buffer.seek(0)

    # load
    try:
        cursor.copy_from(buffer, tablename, sep="\t", null='')
    except psycopg2.errors.UniqueViolation:
        log.info('duplicate, continuing')
    except psycopg2.errors.InFailedSqlTransaction:
        cursor.execute('ROLLBACK')

def main():
    parser = argparse.ArgumentParser(description='Loads a directory of JSON lines files to a database')
    parser.add_argument(
        '--input',
        help='Path to the folder',
        type=str,
        required=True)
    parser.add_argument(
        '--host',
        help='Database host',
        type=str,
        default='localhost')
    parser.add_argument(
        '--port',
        help='Database port',
        type=str,
        default=5432)
    parser.add_argument(
        '--user',
        help='Database user',
        type=str,
        default='postgres')
    parser.add_argument(
        '--password',
        help='Database password',
        type=str,
        default=None)
    parser.add_argument(
        '--dbname',
        help='Database name',
        type=str,
        default='parler')
    parser.add_argument(
        '--table',
        help='Table name',
        type=str,
        default='posts')
    args = parser.parse_args()

    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)

    s_time = time.time()
    print("Starting dataset loading")
    with tqdm(desc="Loading rows", unit=" rows") as pbar:
        files = glob.glob(f"{args.input}/*.ndjson")
        for file in files:
            load_ndjson_file(
                filename=file,
                connection=engine.raw_connection(),
                tablename=args.table,
                pbar=pbar
            )
            rows_loaded = pbar.n
        print(f"{rows_loaded} rows loaded. Operation finished in {time.time() - s_time:.2f} seconds.")

if __name__ == '__main__':
    main()
