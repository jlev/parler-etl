import os, sys, time
import argparse
import csv
import logging

import boto3, botocore
import sqlalchemy
import psycopg2
from tqdm import tqdm

log = logging.getLogger(__name__)

def parse_input(filename):
    data = []
    with open(filename) as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def get_posts(connection, username):
    rows = connection.execute('''
        SELECT users.username, posts.body, posts.created_at, posts.impression_count, posts.links
        FROM posts
        JOIN users on posts.creator = users.id
        WHERE users.username = '%s';''' % username)
    return [r for r in rows]

POSTS_HEADER = ['username', 'body', 'created_at', 'impressions', 'links']

def get_bio(connection, username):
    rows = connection.execute('''
        SELECT users.username, users.banned, users.bio, users.followers, users.following, users.joined, users.verified
        FROM users
        WHERE users.username = '%s';''' % username)
    return [r for r in rows]

BIOS_HEADER = ['username', 'banned', 'bio', 'followers', 'following', 'joined', 'verified']

def get_video(bucket, key, filename):
    try:
        bucket.download_file(key, filename, ExtraArgs={'RequestPayer':'requester'})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"The file {key} does not exist.")
        elif e.response['Error']['Code'] == "404":
            print(f"Forbidden. Did you pass access and secret?")
        else:
            raise

def write_output(header, data, filename):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description='Exports all posts, videos and bio for a given list of users')
    
    parser.add_argument(
        '--input',
        help='Path to the csv of usernames',
        type=str,
        required=True)
    parser.add_argument(
        '--output',
        help='Path to the folder',
        default='.',
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
        '--aws_key',
        help='AWS key',
        type=str,
        default=None)
    parser.add_argument(
        '--aws_secret',
        help='AWS secret',
        type=str,
        default=None)
    parser.add_argument(
        '--dbname',
        help='Database name',
        type=str,
        default='parler')
    args = parser.parse_args()
    s_time = time.time()

    # connect to database
    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)
    connection = engine.connect()

    # connect to s3
    s3 = boto3.resource('s3',
        aws_access_key_id=args.aws_key,
        aws_secret_access_key=args.aws_secret,
    )

    # parse input
    in_data = parse_input(args.input)
    users = list(set([d.get('username') for d in in_data]))
    videos = list(set([d.get('metadata_id') for d in in_data]))
    # prepare output directory
    if not os.path.exists(args.output):
        os.mkdir(args.output)
        os.mkdir(f"{args.output}/posts")
        os.mkdir(f"{args.output}/videos")

    with tqdm(desc="Exporting posts", unit=" posts", total=len(users)) as progress:
        bios = []
        for user in users:
            if not user:
                continue
            bios.append(get_bio(connection, user))
            posts = get_posts(connection, user)
            write_output(POSTS_HEADER, posts, f"{args.output}/posts/{user}.csv")
            progress.update(1)
        write_output(BIOS_HEADER, bios, f"{args.output}/bios.csv")

    with tqdm(desc="Exporting videos", unit=" videos", total=len(videos)) as progress:
        bucket = s3.Bucket('ddosecrets-parler')
        for video in videos:
            get_video(bucket, video, f"{args.output}/videos/{video}.mp4")
            progress.update(1)

    print(f"{len(users)} exported. Operation finished in {time.time() - s_time:.2f} seconds.")

if __name__ == '__main__':
    main()
