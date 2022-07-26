#!/usr/bin/env python

import argparse
from copy import copy
import csv
from datetime import datetime
import gzip
from hashlib import md5
from json import loads
import os
import sys
import urllib.parse

from pyarrow import BufferReader, csv as pa_csv, orc, parquet
import boto3
import botocore.exceptions


def main(manifest_location: str,
         max_depth: int,
         out_file: str,
         cache_dir: str) -> None:

    manifest = load_manifest(manifest_location)
    results = process_investory(manifest, max_depth, cache_dir)

    if out_file:
        print(f"Writing results to out file {out_file}")
        with open(out_file, "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow((
                "Folder",
                "Count",
                "Size",
                "DelSize",
                "VerSize",
                "AvgObject",
                ))

            for folder, details in results.items():
                writer.writerow((
                    urllib.parse.unquote(folder),
                    details["Count"],
                    details["Size"],
                    details["DelSize"],
                    details["VerSize"],
                    details["AvgObj"],
                ))
    else:
        print_results(results)


def convert_bytes(size: int, unit=None) -> str:
    if unit == "K":
        return str(round(size / 1024, 3)) + ' KB'
    elif unit == "M":
        return str(round(size / (1024 * 1024), 3)) + ' MB'
    elif unit == "G":
        return str(round(size / (1024 * 1024 * 1024), 3)) + ' GB'
    else:
        return str(size) + ' Bytes'


def load_manifest(location: str) -> dict:
    print('Loading Inventory Manifest')

    if not location.endswith("/"):
        # Handle if the JSON file is supplied rather than the folder prefix
        location = location.replace("/manifest.json", "") + "/"

    bucket, prefix = parse_bucket_name(location)
    json_key = prefix + "manifest.json"
    checksum_key = prefix + 'manifest.checksum'

    s3 = boto3.client("s3")
    try:
        manifest_json = s3.get_object(Bucket=bucket,
                                      Key=json_key).get('Body').read()
        manifest_checksum = s3.get_object(Bucket=bucket,
                                          Key=checksum_key).get('Body').read()
    except botocore.exceptions.ClientError as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)

    if md5(manifest_json).hexdigest() != manifest_checksum.decode().rstrip("\n"):
        raise AssertionError('The manifest failed the MD5 Check')

    return loads(manifest_json)


def collect_data(s3: boto3.client,
                 bucket: str,
                 file_spec: dict,
                 cache_dir: str) -> bytes:

    if cache_dir and cache_dir.endswith("/"):
        cache_dir = cache_dir.rstrip("/")

    local_path = cache_dir + file_spec['key'][file_spec['key'].rindex("/"):]

    if cache_dir and not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    if cache_dir and os.path.isfile(local_path):
        print('Local:', local_path)
        with open(local_path, 'rb') as file:
            data = file.read()
    else:
        print('S3:', file_spec['key'])
        file_object = s3.get_object(Bucket=bucket, Key=file_spec["key"])
        data = file_object.get('Body').read()

        if md5(data).hexdigest() != file_spec['MD5checksum']:
            raise AssertionError('The inventory file failed the MD5 Check')

        if cache_dir:
            with open(local_path, 'wb') as file:
                file.write(data)

    return data


def process_investory(manifest: dict, max_depth: int, cache_dir: str) -> dict:
    inventory_bucket = manifest['destinationBucket'].split(":::")[1]
    template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0}
    folders = {"/": copy(template)}
    object_count = 0

    print("Processing Inventory")
    start = datetime.now()
    s3 = boto3.client("s3")

    for file in manifest["files"]:
        data = collect_data(s3, inventory_bucket, file, cache_dir)

        if manifest["fileFormat"] == 'Parquet':
            table = parquet.read_table(BufferReader(data))
        elif manifest["fileFormat"] == 'ORC':
            table = orc.read_table(BufferReader(data))
        elif manifest["fileFormat"] == 'CSV':
            csv_names = [
                "bucket",
                "key",
                "version_id",
                "is_latest",
                "is_delete_marker",
                "size",
            ]
            read_options = pa_csv.ReadOptions(column_names=csv_names)
            table = pa_csv.read_csv(BufferReader(gzip.decompress(data)),
                                    read_options)
        else:
            raise ValueError("Unknown file format")

        for key, is_latest, is_delete, size in zip(
                table['key'],
                table['is_latest'],
                table['is_delete_marker'],
                table['size']):

            object_count += 1
            folder = key.as_py()
            size = size.as_py()

            if not size:
                size = 0

            folder_count = folder.count("/")
            depth = folder_count if max_depth is None or \
                folder_count <= max_depth else max_depth

            trim_base = 0
            for index in range(1, depth + 1):
                trim_point = folder.index("/", trim_base)
                trim_base = trim_point + 1
                entry = "/" if index == 1 else folder[:trim_base]

                if entry not in folders:
                    folders[entry] = copy(template)

                folders[entry]["Count"] += 1
                folders[entry]["Size"] += size

                if not is_latest.as_py():
                    folders[entry]["VerSize"] += size

                if is_delete.as_py():
                    folders[entry]["DelSize"] += size
        break
    duration = datetime.now() - start
    print("Processed %d objects in %d seconds\n" % (object_count,
                                                    duration.seconds))

    for folder, details in folders.items():
        folders[folder]["AvgObj"] = round(details["Size"] / details["Count"])

    return folders


def print_results(results: dict) -> None:
    print(f"{'Count':>15} |"
          f"{'Total Size':>16} |"
          f"{'Ver Size':>16} |"
          f"{'Del Size':>16} |"
          f"{'Avg Object':>16} |"
          " Folder\n", "-" * 110)

    for folder, details in results.items():
        print(f"{details['Count']:>15} |",
              f"{convert_bytes(details['Size'], 'G'):>15} |",
              f"{convert_bytes(details['DelSize'], 'G'):>15} |",
              f"{convert_bytes(details['VerSize'], 'G'):>15} |",
              f"{convert_bytes(details['AvgObj'], 'K'):>15} |",
              urllib.parse.unquote(folder))


def parse_bucket_name(name: str) -> tuple:
    name = name.lstrip("s3://")
    return (name[:name.index("/")], name[name.index("/"):].lstrip("/"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 Inventory Report")
    parser.add_argument("-c", default="", dest="cache_dir",
                        help="Folder name for caching data files")
    parser.add_argument("-d", type=int, dest="max_depth",
                        help="The maximum depth folder to calculate sizing")
    parser.add_argument("-m", dest="manifest", required=True,
                        help="The S3 Folder storing the manifest file")
    parser.add_argument("-o", dest="out_file",
                        help="The local or S3 location to write the report")
    args = parser.parse_args()

    try:
        main(args.manifest, args.max_depth, args.out_file, args.cache_dir)
    except KeyboardInterrupt:
        print("\nExiting", file=sys.stderr)
        sys.exit(1)
