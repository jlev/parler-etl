import os
import argparse
import orjson, json
import time
import csv
import io
import re
import logging

from datetime import datetime
import sqlalchemy
import psycopg2
from tqdm import tqdm

log = logging.getLogger(__name__)

def load_jsonl_file(filename, connection, tablename, pbar):
    with open(filename, 'r') as file:
        cursor = connection.cursor()
        lines_loaded = 0

        for line in file:
            row = transform_row(orjson.loads(line))
            copy_to_database(row, cursor, tablename)
            pbar.update(1)
            if lines_loaded % 10:
                connection.commit()
            lines_loaded += 1

        return lines_loaded

def convert_dms_to_decimal(dms):
    # ex: "44 deg 57' 24.12\" N"
    dms = dms.replace(" deg ", " ")
    dms = dms.replace("\'", "")
    dms = dms.replace("\"", "")

    degrees, minutes, seconds, direction = dms.split()
    dd = (float(degrees) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
    return dd

def transform_row(row):
    # don't create sqlalchemy models here, just match the defined format and order

    # eg "2021:01:08 21:01:04"
    created_at = None
    if row.get('CreateDate'):
        try:
            created_at = datetime.strptime(row.get('CreateDate'), "%Y:%m:%d %H:%M:%S")
        except ValueError:
            log.info(f"unable to parse date from {row['CreateDate']}")

    return [
        row.get('video_id'), 
        created_at,
        convert_dms_to_decimal(row.get('GPSLatitude')) if row.get('GPSLatitude') else None,
        convert_dms_to_decimal(row.get('GPSLongitude')) if row.get('GPSLongitude') else None,
        json.dumps(row), # maintain json string
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
    parser = argparse.ArgumentParser(description='Loads single JSON lines file to a database')
    parser.add_argument(
        '--input',
        help='Path to the JSON file',
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
        default='metadata')
    args = parser.parse_args()

    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)

    s_time = time.time()
    print("Starting metadata loading")
    with tqdm(desc="Loading rows", unit=" rows") as pbar:
        rows_loaded = load_jsonl_file(args.input, engine.raw_connection(), args.table, pbar)
    print(f"{rows_loaded} rows loaded. Operation finished in {time.time() - s_time:.2f} seconds.")


if __name__ == '__main__':
    main()
