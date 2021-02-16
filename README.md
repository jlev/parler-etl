# Parler ETL
## Scripts to extract, transform and load Parler data

Requirements:
- Python3
- `pip install -r requirements.txt`

Steps:
- Download mirror of posts (34.2gb), users (1.1gb), metadata (200mb)
- unzip
- `python create-data-tables.py --host --username --password`
- `python load-posts-to-database.py --input parler_posts.zip --host --username --password`
- `python load-users-to-database.py --input parler_users.zip --host --username --password`
- `python transform-video-metadata-json-to-jsonl.py --input metadata.tar.gz --output metadata.jsonl`
- `python load-metadata-to-database.py --input metadata.jsonl --host --username --password`

Acknowledgements:
- donk_enby for scraping the metadata
- https://zenodo.org/record/4442460#.YCr7uS1h1f2 for hosting the posts and users dataset
- jnissin for the transform scripts
