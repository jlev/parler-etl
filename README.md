# Parler ETL
## Scripts to extract, transform and load Parler data

Requirements:
- Python3
- `pip install -r requirements.txt`

Steps:
- Download mirror of posts archive (20gb), metadata (200mb)
- `python transform-post-html-to-jsonl.py --input posts.zip --output posts.jsonl`
- wait... (this took ~3hrs on a EC2 m5.4xlarge, resulting file was 1.7gb)
- `python transform-video-metadata-json-to-jsonl.py --input metadata.tar.gz --output metadata.jsonl`
- wait... (this took ~5min)
- `python create-data-tables.py --host --username --password`
- `python load-posts-to-database.py --input posts.jsonl --host --username --password`
- `python load-metadata-to-database.py --input metadata.jsonl --host --username --password`

Acknowledgements:
- donk_enby for scraping the original dataset
- mirrors for hosting after takedown
- jnissin for the transform scripts
