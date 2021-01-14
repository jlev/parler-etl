import os
import argparse
import logging
import orjson
import multiprocessing
import time
import zipfile

from typing import BinaryIO
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from parsel import Selector


def read_html_file(
        file_path: str) -> str:
    """
    Reads a HTML file and returns it as a string.

    :param file_path: file path
    :return: HTML file as a string
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        str_html = "\n".join(f.readlines())
    return str_html


def get_all_files_in_zip(
        zip_path: str):

    with zipfile.ZipFile(zip_path, mode='r') as zip_file:
        zip_path = zipfile.Path(zip_file)

        for zip_file_path in zip_path.iterdir():
            file_contents = zip_file_path.read_text()
            yield zip_file_path.name, file_contents


def process_html_file(
        html_file_path: str,
        html_file_contents: str,
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

    # Read and parse the HTML file
    str_html = html_file_contents #read_html_file(html_file_path)
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

    # Extract all the different counts
    int_impression_count = int(sel_post_container.css('span.impressions--count::text').get())
    sel_counts = sel_post_container.css('div.card--footer div.pa--item--wrapper')
    counts = dict()

    for sel_count in sel_counts:
        str_count_name = sel_count.css('img').xpath('@alt').get()
        if str_count_name:
            str_count_name = str_count_name.lower().replace(' ', '_')
            int_count = int(sel_count.css('span.pa--item--count::text').get())
            counts[str_count_name] = int_count

    video_urls = list(set(sel_root.css('div.mc-video--meta--wrapper > span.mc-video--link > a').xpath('@href').getall()))
    image_urls = list(set(sel_root.css('div.media-container--wrapper > div.mc-image--container').css('img').xpath('@src').getall()))
    sel_iframes = sel_root.css('div.mc-iframe-embed--container')
    iframe_media = []

    for sel_iframe in sel_iframes:
        iframe_media.append(
            {
                'source_url': sel_iframe.css('iframe').xpath('@src').get(),
                'meta_title': sel_iframe.css('span.mc-iframe-embed--title::text').get(),
                'meta_excerpt': sel_iframe.css('span.mc-iframe-embed--excerpt::text').get(),
                'meta_link': sel_iframe.css('span.mc-iframe-embed--link > a').xpath('@href').get()
            })

    # Build the post object
    post = {
        'id': str_post_id,
        'author_username': str_author_username,
        'author_name': str_author_name,
        'author_profile_img_url': str_author_profile_img_url,
        'title': str_title,
        'created_at': str_created_at,
        # Original post information if this is an echo
        'is_echo': bool_is_echo,
        'echo_author_username': str_echo_author_username,
        'echo_author_profile_img_url': str_echo_author_profile_img_url,
        'echo_author_name': str_echo_author_name,
        'echo_created_at': str_echo_created_at,
        # End of original post block
        'body': str_body,
        'impression_count': int_impression_count,
        'comment_count': counts.get('post_comments', -1),
        'echo_count': counts.get('post_echoes', -1),
        'upvote_count': counts.get('post_upvotes', -1),
        'video_urls': video_urls,
        'image_urls': image_urls,
        'iframe_media': iframe_media
    }

    str_post_json = orjson.dumps(post, option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_NAIVE_UTC)
    output_fp.write(str_post_json)

    if pbar is not None:
        pbar.update(1)


def process_dataset(
        input_path: str,
        output_path: str) -> int:
    """
    Processes the parler HTML posts dataset into a JSON lines format.

    :param input_path: path to the directory cotaining the HTML files
    :param output_path: path to the output JSON lines document preferably with '.json' extension
    :return: the number of processed files
    """

    with open(output_path, mode='wb') as output_fp:
        with tqdm(desc="Transforming HTML files", unit=" HTML files") as pbar:
            max_workers = max(multiprocessing.cpu_count() - 1, 1)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda f: process_html_file(
                    html_file_path=f[0],
                    html_file_contents=f[1],
                    output_fp=output_fp,
                    pbar=pbar), get_all_files_in_zip(input_path))
            num_processed = pbar.n
    return num_processed


def main():
    # TODO: LOGGING!!
    parser = argparse.ArgumentParser(description='Preprocess the Parler HTML posts into a single JSON lines file.')
    parser.add_argument(
        '--input',
        help='Path to the folder with the HTML files',
        type=str,
        required=True)
    parser.add_argument(
        '--output',
        help='Path to the output JSON lines document',
        type=str,
        required=True)
    args = parser.parse_args()

    s_time = time.time()
    print("Starting dataset processing")
    num_processed = process_dataset(args.input, args.output)
    print(f"{num_processed} HTML files processed. Operation finished in {time.time() - s_time:.2f} seconds.")


if __name__ == '__main__':
    main()
