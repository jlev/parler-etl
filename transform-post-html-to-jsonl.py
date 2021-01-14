import os
import argparse
import orjson
import multiprocessing
import time
import zipfile
import zipp
import dateparser
import datetime
import re
import threading
import concurrent.futures

from typing import BinaryIO, Optional
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from parsel import Selector


_THREAD_LOCK = threading.Lock()


def get_all_files_in_zip(
        zip_path: str):

    with zipfile.ZipFile(zip_path, mode='r') as zf:
        zp = zipp.Path(zf)

        for zfp in zp.iterdir():
            if zfp.is_file():
                file_contents = zfp.read_text()
                yield zfp.name, file_contents


def parse_datetime(
        timestamp: str,
        time_offset: datetime.timedelta) -> Optional[datetime.datetime]:
    """
    Attempts to parse an approximate datetime from the provided string.

    :param timestamp: a freeform string describing a timestamp
    :param time_offset: a deltatime used to offset dates for timestamps such as ´1 day ago´ or ´2 hours ago´
    :return:
    """
    if timestamp is None:
        return None

    # dateparser is not thread safe
    with _THREAD_LOCK:
        dt_parsed = dateparser.parse(timestamp, languages=['en'])

    try:
        if re.search('[a-zA-Z]', timestamp):
            dt_approx = dt_parsed - time_offset
        else:
            dt_approx = dateparser.parse(timestamp, languages=['en'])
        return dt_approx
    except Exception as e:
        print(f"Failed to parse datetime with timestamp: {timestamp}, offset: {time_offset}: {e}")
        return None
    except KeyboardInterrupt:
        raise


def process_html_file(
        html_file_path: str,
        html_file_contents: str,
        scrape_time_offset: datetime.timedelta,
        output_fp: BinaryIO,
        pbar: tqdm):
    """
    Processes a single Parler post HTML file and writes it to the output as a JSON line.

    :param html_file_path: path to the HTML file
    :param html_file_contents: HTML file as a string
    :param output_fp: Binary file pointer to the output
    :param pbar: Optional TQDM progress bar
    :return: nothing
    """

    # Post ID is assumed to be in the file name
    str_post_id = os.path.basename(html_file_path)

    try:
        # Read and parse the HTML file
        str_html = html_file_contents
        sel_root = Selector(text=str_html)

        # Username can be extracted from the title
        str_title = sel_root.css('title::text').get()
        title_parts = str_title.split('-')
        str_author_username = title_parts[0].strip('@ ') if len(title_parts) > 0 else None
        str_author_name = title_parts[1].strip() if len(title_parts) > 1 else None
        str_title = title_parts[2].strip() if len(title_parts) > 2 else None

        sel_post_container = sel_root.css('div.card--post-container')

        # We can get a better author name from echoed by information if it's available
        str_echoed_by = sel_post_container.css('div.eb--statement > span.reblock::text').get()

        if str_echoed_by:
            str_author_name = str_echoed_by[len('Echoed By'):].strip()

        str_author_profile_img_url = sel_post_container.css('div.eb--profile-pic > img').xpath('@src').get()
        str_created_at = sel_post_container.css('span.card-meta--row > span.post--timestamp::text').get()
        dt_approx_created_at = parse_datetime(str_created_at, scrape_time_offset)

        # Body text - get all paragraphs and their texts
        str_body = "\n".join(sel_post_container.css('div.card--body > p::text').getall())

        # Echo user details
        sel_echo_container = sel_post_container.css('span.echo--parent')
        bool_is_echo = sel_echo_container.get() is not None
        str_echo_author_name = sel_echo_container.css('span.author--name::text').get()
        str_echo_author_username = sel_echo_container.css('span.author--username::text').get()
        str_echo_author_username = str_echo_author_username.strip('@') if str_echo_author_username else None
        str_echo_author_profile_img_url = sel_echo_container.css('div.ch--avatar--wrapper > img').xpath('@src').get()
        str_echo_created_at = sel_echo_container.css('span.post--timestamp::text').get()
        dt_echo_approx_created_at = parse_datetime(str_echo_created_at, scrape_time_offset)

        # Extract all the different counts
        counts = dict()
        int_impression_count = int(sel_post_container.css('span.impressions--count::text').get(-1))
        sel_counts = sel_post_container.css('div.card--footer div.pa--item--wrapper')

        for sel_count in sel_counts:
            str_count_name = sel_count.css('img').xpath('@alt').get()
            if str_count_name:
                str_count_name = str_count_name.lower().replace(' ', '_')
                int_count = int(sel_count.css('span.pa--item--count::text').get())
                counts[str_count_name] = int_count

        # Media parsing
        sel_media = sel_root.css('div.media-container--wrapper')

        # Media parsing: images
        sel_images = sel_media.css('div.mc-image--container')
        media_images = []

        for sel_image in sel_images:
            image_url = sel_image.css('div.mc-image--wrapper > img').xpath('@src').get()
            image_id = os.path.basename(image_url)
            media_images.append({
                'id': image_id,
                'url': image_url
            })

        # Media parsing: videos
        sel_videos = sel_media.css('div.mc-video--container')

        media_videos = []

        for sel_video in sel_videos:
            # Extract the video source information
            video_source_url = sel_video.css('video > source').xpath('@src').get()
            video_source_type = sel_video.css('video > source').xpath('@type').get()
            video_id = os.path.basename(video_source_url).split('.')[0]

            # Extract the metadata
            sel_video_meta = sel_video.css('div.mc-video--meta--wrapper')
            video_url = "".join(sel_video_meta.css('span.mc-video--link > a::text').getall()).strip()
            video_api_url = sel_video_meta.css('span.mc-video--link > a').xpath('@href').get()
            media_videos.append({
                'id': video_id,
                'title': sel_video_meta.css('span.mc-video--title::text').get("").strip(),
                'excerpt': sel_video_meta.css('span.mc-video--excerpt::text').get("").strip(),
                'url': video_url,
                'api_url': video_api_url,
                'source_url': video_source_url,
                'source_type': video_source_type
            })

        # Media parsing: basic links
        sel_basics = sel_media.css('div.mc-basic--meta--wrapper')
        media_basics = []

        for sel_basic in sel_basics:
            media_basics.append({
                'url': "".join(sel_basic.css('a::text').getall()).strip(),
                'api_url': sel_basic.css('a').xpath('@href').get()
            })

        # Media parsing: articles
        sel_articles = sel_media.css('div.mc-article--meta--wrapper')
        media_articles = []

        for sel_article in sel_articles:
            media_articles.append({
                'title': sel_article.css('span.mc-article--title::text').get("").strip(),
                'excerpt': sel_article.css('span.mc-article--excerpt::text').get("").strip(),
                'url': "".join(sel_article.css('span.mc-article--link > a::text').getall()).strip(),
                'api_url': sel_article.css('span.mc-article--link > a').xpath('@href').get()
            })

        # Media parsing: website
        sel_websites = sel_media.css('div.mc-website--meta--wrapper')
        media_websites = []

        for sel_website in sel_websites:
            media_websites.append({
                'title': sel_website.css('span.mc-website--title::text').get("").strip(),
                'excerpt': sel_website.css('span.mc-website--excerpt::text').get("").strip(),
                'url': "".join(sel_website.css('span.mc-website--link > a::text').getall()).strip(),
                'api_url': sel_website.css('span.mc-website--link > a').xpath('@href').get()
            })

        # Media parsing: iframes
        sel_iframes = sel_root.css('div.mc-iframe-embed--container')
        media_iframes = []

        for sel_iframe in sel_iframes:
            media_iframes.append({
                'title': sel_iframe.css('div.mc-iframe-embed--meta--wrapper > span.mc-iframe-embed--title::text').get("").strip(),
                'excerpt': sel_iframe.css('div.mc-iframe-embed--meta--wrapper > span.mc-iframe-embed--excerpt::text').get("").strip(),
                'url': sel_iframe.css('iframe').xpath('@src').get(),
                'api_url': sel_iframe.css('div.mc-iframe-embed--meta--wrapper > span.mc-iframe-embed--link > a').xpath('@href').get()
            })

        # Build the post object
        post = {
            'id': str_post_id,
            'author_username': str_author_username,
            'author_name': str_author_name,
            'author_profile_img_url': str_author_profile_img_url,
            'title': str_title,
            'created_at': str_created_at,
            'approx_created_at': dt_approx_created_at,
            'body': str_body,
            'impression_count': int_impression_count,
            'comment_count': counts.get('post_comments', -1),
            'echo_count': counts.get('post_echoes', -1),
            'upvote_count': counts.get('post_upvotes', -1),
            'is_echo': bool_is_echo,
            'echo': {
                'author_username': str_echo_author_username,
                'author_profile_img_url': str_echo_author_profile_img_url,
                'author_name': str_echo_author_name,
                'created_at': str_echo_created_at,
                'approx_created_at': dt_echo_approx_created_at
            } if bool_is_echo else None,
            'media': {
                'images': media_images,
                'videos': media_videos,
                'basics': media_basics,
                'articles': media_articles,
                'websites': media_websites,
                'iframes': media_iframes
            }
        }

        str_post_json = orjson.dumps(post, option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_NAIVE_UTC)

        with _THREAD_LOCK:
            output_fp.write(str_post_json)

            if pbar is not None:
                pbar.update(1)
    except Exception as ex:
        print(f"Failed to parse post HTML for post ID: {str_post_id}: {ex}")
    except KeyboardInterrupt:
        raise


def process_dataset(
        input_path: str,
        output_path: str,
        scrape_date: str) -> int:
    """
    Processes the Parler HTML posts dataset into a JSON lines format.

    :param input_path: path to the zip file containing the HTML documents
    :param output_path: path to the output JSON lines document preferably with '.json' extension
    :param scrape_date: date of the scraping
    :return: the number of processed files
    """

    max_workers = max(multiprocessing.cpu_count() - 1, 1)
    print(f"Using {max_workers} workers")
    scrape_time_offset = datetime.datetime.now() - dateparser.parse(scrape_date)

    with open(output_path, mode='wb') as output_fp:
        with tqdm(desc="Transforming HTML files", unit=" HTML files") as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                try:
                    executor.map(lambda f: process_html_file(
                        html_file_path=f[0],
                        html_file_contents=f[1],
                        scrape_time_offset=scrape_time_offset,
                        output_fp=output_fp,
                        pbar=pbar
                    ), get_all_files_in_zip(input_path))
                except KeyboardInterrupt:
                    print("CTRL-C received: Shutting down forcibly")
                    executor.shutdown(False)
                    executor._threads.clear()
                    concurrent.futures.thread._threads_queues.clear()

            num_processed = pbar.n
    return num_processed


def main():
    parser = argparse.ArgumentParser(description='Preprocess the Parler HTML posts into a single JSON lines file.')
    parser.add_argument(
        '--input',
        help='Path to the the HTML files zip',
        type=str,
        required=True)
    parser.add_argument(
        '--output',
        help='Path to the output JSON lines document',
        type=str,
        required=True)
    parser.add_argument(
        '--scrape-date',
        help='Date of the scraping YYYY-MM-DD, this allows us parse approximate timestamps',
        type=str,
        default='2021-01-06')
    args = parser.parse_args()

    s_time = time.time()
    print(f"Starting dataset processing using scrape date: {args.scrape_date}")
    num_processed = process_dataset(args.input, args.output, args.scrape_date)
    print(f"{num_processed} HTML files processed. Operation finished in {time.time() - s_time:.2f} seconds.")
    print(f"JSON lines file saved to: {args.output}")


if __name__ == '__main__':
    main()
