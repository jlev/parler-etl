import argparse
import sqlalchemy
import models

def main():
    parser = argparse.ArgumentParser(description='Create data tables')
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
    args = parser.parse_args()

    engine_url = sqlalchemy.engine.url.URL('postgresql',
        username=args.user, password=args.password, host=args.host, port=args.port, database=args.dbname)
    engine = sqlalchemy.create_engine(engine_url)
    with engine.connect() as connection:
        models.base.metadata.create_all(engine)
    
if __name__ == '__main__':
    main()
