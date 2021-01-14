import os
import argparse
import orjson
import multiprocessing
import time
import tarfile

from typing import BinaryIO
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def get_all_files_in_tar_gz(
        tar_gz_path: str,
        include_hidden: bool = False,
        file_extension: str = None) -> tuple:
    """
    A generator function that iterates all the files in the given tar.gz file.

    :param dir_path: path to the root of the directory tree
    :param include_hidden: include hidden files (starting with '.')
    :param file_extension: only return files with the file extension
    :return: tuple(file path, file content)
    """

    with tarfile.open(tar_gz_path, mode='r') as targz_file:
        tar_info = targz_file.next()

        while tar_info is not None:
            # If the tar info does not satisfy the conditions continue until we find one that does
            while (not include_hidden and tar_info.name.startswith('.')) or \
                    (file_extension is not None and not tar_info.name.endswith(file_extension)):
                tar_info = targz_file.next()

            if tar_info is not None:
                file_contents = targz_file.extractfile(tar_info).read()
                yield tar_info.name, file_contents
                tar_info = targz_file.next()


def process_json_file(
        json_file_path: str,
        json_file_contents: str,
        output_fp: BinaryIO,
        pbar: tqdm):
    """
    Processes a single Parler post HTML file and writes it to the output as a JSON line.

    :param json_file_path: path to the JSON file
    :param json_file_contents: JSON file as a string
    :param output_fp: Binary file pointer to the output
    :param pbar: Optional TQDM progress bar
    :return: nothing
    """

    # Video ID is assumed to be in the file name
    str_video_id = os.path.basename(json_file_path)
    str_video_id = str_video_id[len('meta-'):-len('.json')]

    # Load the video metadata and add the
    video_metadata = orjson.loads(json_file_contents)[0]
    video_metadata['video_id'] = str_video_id

    str_video_metadata = orjson.dumps(
        video_metadata,
        option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_NAIVE_UTC)
    output_fp.write(str_video_metadata)

    if pbar is not None:
        pbar.update(1)


def process_dataset(
        input_path: str,
        output_path: str) -> int:
    """
    Processes the Parler video metadata dataset into a JSON lines format.

    :param input_path: path to the tar.gz containing the video metadata JSON files
    :param output_path: path to the output JSON lines document preferably with '.json' extension
    :return: the number of processed files
    """

    with open(output_path, mode='wb') as output_fp:
        with tqdm(desc="Transforming JSON metadata files", unit=" JSON files") as pbar:
            max_workers = max(multiprocessing.cpu_count() - 1, 1)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda f: process_json_file(
                    json_file_path=f[0],
                    json_file_contents=f[1],
                    output_fp=output_fp,
                    pbar=pbar), get_all_files_in_tar_gz(input_path, file_extension='.json'))
            num_processed = pbar.n
    return num_processed


def main():
    parser = argparse.ArgumentParser(description='Preprocess the Parler video metadata JSON files into a single JSON lines file.')
    parser.add_argument(
        '--input',
        help='Path to the tar.gz with the video metadata JSON files',
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
    print(f"{num_processed} JSON metadata files processed. Operation finished in {time.time() - s_time:.2f} seconds.")


if __name__ == '__main__':
    main()
