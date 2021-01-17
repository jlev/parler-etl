import os
import argparse
import orjson, json
import time
import csv
import io

import sqlalchemy
from tqdm import tqdm

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

def transform_row(row):
    # don't create sqlalchemy models here, just match the defined format and order
    return [
        row['id'], 
        row['author_username'],
        row['author_name'],
        row['author_profile_img_url'],
        row['title'],
        row['created_at'],
        row['approx_created_at'],
        row['body'],
        row['impression_count'],
        row['comment_count'],
        row['echo_count'],
        row['upvote_count'],
        row['is_echo'],
        json.dumps(row['echo']) if row['echo'] else None, # maintain json string
        json.dumps(row['media']) if row['media'] else None
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
    cursor.copy_from(buffer, tablename, sep="\t", null='')

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
        default='posts')
    args = parser.parse_args()

    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)

    s_time = time.time()
    print("Starting dataset loading")
    with tqdm(desc="Loading rows", unit=" rows") as pbar:
        rows_loaded = load_jsonl_file(args.input, engine.raw_connection(), args.table, pbar)
    print(f"{rows_loaded} rows loaded. Operation finished in {time.time() - s_time:.2f} seconds.")


if __name__ == '__main__':
    main()
