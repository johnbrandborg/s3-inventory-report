#!/usr/bin/env python

import csv
from datetime import datetime
import gzip
from  hashlib import md5
from json import loads
import os
from threading import Thread
from time import sleep
import sys
import urllib.parse

from pyarrow import BufferReader, parquet
from boto3 import client
import botocore.exceptions

s3 = client('s3')


def cli():
    bucket_name = None
    max_depth = None
    out_file = None

    if len(sys.argv) == 1:
        print("S3 Folder Size Report\n" + "-" * 40 + "\n",
              "-m s3://<bucket_name>/<prefix>/\n",
              "-d <max_depth>  Optional",
              "-o <file> Optional")
        return 0

    for index, argv in enumerate(sys.argv):
        if "-m" == argv:
            manifest_location = sys.argv[index + 1]
        if "-d" == argv:
            try:
                max_depth = int(sys.argv[index + 1])
            except ValueError:
                print("ERROR: Maximum depth needs to be an integer",
                     file=sys.stderr)
                return 1
        if "-o" == argv:
            out_file = sys.argv[index + 1]

    if not manifest_location:
        print("ERROR: An S3 location for the manifest must be supplied",
             file=sys.stderr)
        return 1

    manifest = load_manifest(manifest_location)

    try:
        results = process_investory(manifest, max_depth)
    except KeyboardInterrupt:
        print("\nExiting", file=sys.stderr)
        return 1

    if out_file:
        print(f"Writing results to out file {out_file}")
        with open(out_file, "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(("Folder", "Size", "Count"))

            for key, value in results.items():
                record = (key, value["Size"], value["Count"])
                writer.writerow(record)
    else:
        print_results(results)


def convert_bytes(size, unit=None):
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
        location = location + "/"
    bucket, prefix = parse_bucket_name(location)

    checksum_key = prefix + 'manifest.checksum'
    json_key = prefix + 'manifest.json'

    try:
        manifest_json = s3.get_object(Bucket=bucket, Key=json_key).get('Body').read()
        manifest_checksum = s3.get_object(Bucket=bucket, Key=checksum_key).get('Body').read()
    except botocore.exceptions.ClientError as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)

    if md5(manifest_json).hexdigest() != manifest_checksum.decode().rstrip("\n"):
        raise SystemError('The manifest failed the MD5 Check')

    return loads(manifest_json)


def process_investory(manifest, max_depth) -> dict:
    top_level_folders = {"/": {"Size": 0, "Count": 0}}
    object_count = 0

    print("Processing Inventory", file=sys.stderr)
    start = datetime.now()

    for file in manifest["files"]:
        inventory_bucket = manifest['destinationBucket'].split(":::")[1]
        print(file['key'])
        file_object = s3.get_object(Bucket=inventory_bucket, Key=file["key"])
        data = file_object.get('Body').read()

        if md5(data).hexdigest() != file['MD5checksum']:
            raise SystemError('The inventory file failed the MD5 Check')

        table = parquet.read_table(BufferReader(data))

        for key, size in zip(table['key'], table['size']):
            object_count += 1

            folder = key.as_py()
            size = size.as_py()

            folder_count = folder.count("/")
            depth = folder_count if max_depth == None or \
                folder_count <= max_depth else max_depth

            trim_base = 0
            for index in range(1, depth + 1):
                trim_point = folder.index("/", trim_base)
                trim_base = trim_point + 1
                entry = folder[:trim_base] 

                if index == 1:
                    top_level_folders["/"]["Size"] += size
                    top_level_folders["/"]["Count"] += 1

                try:
                    top_level_folders[entry]["Size"] += size
                    top_level_folders[entry]["Count"] += 1
                except KeyError:
                    top_level_folders[entry] = {"Size": size, "Count": 1}

    duration = datetime.now() - start
    print("Processed %d objects in %d seconds\n" % (object_count,
                                                   duration.seconds),
         file=sys.stderr)

    return top_level_folders


def print_results(results: dict) -> None:
    print(f"{'Total Size':>15} |{'Average':>16} |{'Count':>16} | Folder\n",
         "-" * 80)

    for folder, details in results.items():
        print(f"{convert_bytes(details['Size'], 'G'):>15} | "
              f"{convert_bytes(details['Size'] / details['Count'], 'K'):>15} |",
              f"{details['Count']:>15} |",
              urllib.parse.unquote(folder))


def parse_bucket_name(name):
    name = name.lstrip("s3://")
    return (name[:name.index("/")], name[name.index("/"):].lstrip("/"))


if __name__ == "__main__":
    sys.exit(cli())

