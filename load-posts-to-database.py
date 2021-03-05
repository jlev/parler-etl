import os, sys, glob
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


def load_ndjson_file(filename, type, connection, tablename, pbar):
    with open(filename, 'r') as file:
        cursor = connection.cursor()
        lines_loaded = 0

        data = []
        for line in file:
            jsonline = orjson.loads(line)
            if not jsonline.get('id'):
                print('no id, skipping')
                continue
            if type == 'users':
                row = transform_user(jsonline)
            elif type == 'posts':
                row = transform_post(jsonline)
            else:
                log.error('unknown type')
                row = []
            data.append(row)
            lines_loaded += 1
            pbar.update(1)
        copy_to_database(data, cursor, tablename)
        connection.commit()

        return lines_loaded

def transform_post(row):
    # don't create sqlalchemy models here, just match the defined format and order
    return [
        row['id'], 
        row.get('creator',''),
        datetime.datetime.strptime(row['createdAt'],"%Y%m%d%H%M%S") if 'createdAt' in row else '',
        row.get('body',''),
        row.get('impressions',0),
        row.get('comments',0),
        row.get('upvotes',0),
        json.dumps(row['links']) if row['links'] else None # maintain json string
    ]

def transform_user(row):
    # don't create sqlalchemy models here, just match the defined format and order
    return [
        row['id'], 
        row.get('username',''),
        row.get('banned',False),
        row.get('bio',''),
        row.get('profilePhoto',''),
        row.get('user_followers',0),
        row.get('user_following',0),
        row.get('posts',0),
        datetime.datetime.strptime(row['joined'],"%Y%m%d%H%M%S") if 'joined' in row else '',
        row.get('verified',False),
    ]

def copy_to_database(data, cursor, tablename):
    # use raw cursor copy for speed
    
    # setup buffer
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
    for row in data:
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
        '--type',
        help='users or posts',
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

    if args.type not in ['users', 'posts']:
        log.error('type must be users or posts')
        sys.exit(-1)

    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)

    s_time = time.time()
    print("Starting dataset loading")
    rows_loaded = 0
    with tqdm(desc="Loading files", unit=" files", position=0) as progress:
        files = glob.glob(f"{args.input}/*.ndjson")
        for file in files:
            with tqdm(desc="Transforming rows", unit=" rows", position=1) as pbar:
                load_ndjson_file(
                    filename=file,
                    type=args.type,
                    connection=engine.raw_connection(),
                    tablename=args.table,
                    pbar=pbar
                )
            rows_loaded += pbar.n
        progress.update(1)
        print(f"{rows_loaded} rows loaded. Operation finished in {time.time() - s_time:.2f} seconds.")

if __name__ == '__main__':
    main()
