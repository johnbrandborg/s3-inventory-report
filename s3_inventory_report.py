#!/usr/bin/env python

import argparse
import gzip
import os
import sys
import urllib.parse
from copy import copy
from datetime import datetime
from hashlib import md5
from json import loads

import boto3
import botocore.exceptions
from pyarrow import BufferReader, Table, csv, orc, parquet


# Batches Parquet Files for lower memory usage
LOW_MEMORY = True


def main(manifest_location: str, max_depth: int, out_file: str, cache_dir: str) -> None:
    """
    Main function for the CLI to interact with.  Could be used to Interact
    with from other modules too.
    """
    manifest = load_manifest(manifest_location)
    results = process_investory(manifest, max_depth, cache_dir)

    if out_file:
        write_results(results, out_file)
    else:
        print_results(results)


def convert_bytes(size: int, unit=None) -> str:
    """
    Takes a integer and converts it to a data 'size' formatted string
    """
    if unit == "K":
        return str(round(size / 1024, 3)) + " KB"

    if unit == "M":
        return str(round(size / (1024 * 1024), 3)) + " MB"

    if unit == "G":
        return str(round(size / (1024 * 1024 * 1024), 3)) + " GB"

    return str(size) + " Bytes"


def load_manifest(location: str) -> dict:
    """
    Loads the S3 Inventory manifest file, and performs a MD5 check. Once
    completed, the JSON is deserialised into a Dictionary.
    """
    print("Loading Inventory Manifest")

    if not location.endswith("/"):
        # Handle if the JSON file is supplied rather than the folder prefix
        location = location.replace("/manifest.json", "") + "/"

    bucket, prefix = parse_bucket_url(location)
    json_key = prefix + "manifest.json"
    checksum_key = prefix + "manifest.checksum"

    s3 = boto3.client("s3")
    try:
        manifest_json = s3.get_object(Bucket=bucket, Key=json_key).get("Body").read()
        manifest_checksum = (
            s3.get_object(Bucket=bucket, Key=checksum_key).get("Body").read()
        )
    except botocore.exceptions.ClientError as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)

    if md5(manifest_json).hexdigest() != manifest_checksum.decode().rstrip("\n"):
        raise AssertionError("The manifest failed the MD5 Check")

    return loads(manifest_json)


def collect_data(
    s3: boto3.client, bucket: str, file_spec: dict, cache_dir: str
) -> bytes:
    """
    Downsload files from S3 and returns there contents.  If a cache directory
    is supplied the file is stored locally.
    """
    if cache_dir and cache_dir.endswith("/"):
        cache_dir = cache_dir.rstrip("/")

    local_path = cache_dir + file_spec["key"][file_spec["key"].rindex("/") :]

    if cache_dir and not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    if cache_dir and os.path.isfile(local_path):
        print(local_path)
        with open(local_path, "rb") as file:
            data = file.read()
    else:
        print(f"s3://{bucket}/{file_spec['key']}")
        file_object = s3.get_object(Bucket=bucket, Key=file_spec["key"])
        data = file_object.get("Body").read()

        if md5(data).hexdigest() != file_spec["MD5checksum"]:
            raise AssertionError("The inventory file failed the MD5 Check")

        if cache_dir:
            with open(local_path, "wb") as file:
                file.write(data)

    return data


def process_investory(manifest: dict, max_depth: int, cache_dir: str) -> dict:
    """
    Takes the S3 Inventory Manifest and downloads the necessary files. With
    the data the folder references are aggregrated, are the results returned.
    """
    inventory_bucket = manifest["destinationBucket"].split(":::")[1]
    template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0, "Depth": 0}
    folders = {"/": copy(template)}
    objects = 0

    print("Processing Inventory")
    start = datetime.now()
    s3 = boto3.client("s3")
    needed_columns = ["key", "is_latest", "is_delete_marker", "size"]

    for file in manifest["files"]:
        data = collect_data(s3, inventory_bucket, file, cache_dir)

        if manifest["fileFormat"] == "Parquet" and LOW_MEMORY:
            parquet_file = parquet.ParquetFile(BufferReader(data))

            for table in parquet_file.iter_batches(columns=needed_columns):
                objects += aggregate_folders(table, folders, max_depth, template)
            continue

        if manifest["fileFormat"] == "Parquet":
            table = parquet.read_table(BufferReader(data), columns=needed_columns)
        elif manifest["fileFormat"] == "ORC":
            table = orc.read_table(BufferReader(data), columns=needed_columns)
        elif manifest["fileFormat"] == "CSV":
            csv_names = [
                "bucket",
                "key",
                "version_id",
                "is_latest",
                "is_delete_marker",
                "size",
            ]
            table = csv.read_csv(
                BufferReader(gzip.decompress(data)),
                csv.ReadOptions(column_names=csv_names),
                convert_options=csv.ConvertOptions(include_columns=needed_columns),
            )
        else:
            raise TypeError("Only Parquet, ORC, and CSV formats are supported")

        objects += aggregate_folders(table, folders, max_depth, template)

    duration = datetime.now() - start
    print(f"Processed {objects} objects in {duration.seconds} seconds\n")

    for details in folders.values():
        details["AvgObj"] = round(details["Size"] / details["Count"])

    return folders


def aggregate_folders(
    table: Table, folders: dict, max_depth: int, template: dict
) -> int:
    """
    A pyarrow Table of the s3 inventory data is taken and aggregated by augmenting
    a dictionary that holds the folder data.  If a max depth is supplied it will
    be honored, otherwise all folders are aggregated.

    The number of objects aggregated is counted and returned.
    """
    count = 0

    for key, is_latest, is_delete, size in zip(
        table["key"], table["is_latest"], table["is_delete_marker"], table["size"]
    ):
        count += 1
        folder = key.as_py()
        size = size.as_py()

        if not size:
            size = 0

        depth = folder.count("/")
        if isinstance(max_depth, int) and depth >= max_depth:
            depth = max_depth

        trim_base = 0
        for depth in range(0, depth + 1):
            if depth > 0:
                trim_point = folder.index("/", trim_base)
                trim_base = trim_point + 1
                entry = folder[:trim_base]
            else:
                entry = "/"

            if entry not in folders:
                folders[entry] = copy(template)
                folders[entry]["Depth"] = depth

            folders[entry]["Count"] += 1
            folders[entry]["Size"] += size

            if not is_latest.as_py():
                folders[entry]["VerSize"] += size

            if is_delete.as_py():
                folders[entry]["DelSize"] += size

    return count


def parse_bucket_url(url: str) -> tuple:
    """
    Takes a S3 URL and returns a tuple containing the bucket, and key
    """
    url = url.lstrip("s3://")
    return (url[: url.index("/")], url[url.index("/") :].lstrip("/"))


def print_results(results: dict) -> None:
    """
    Console print the data from the inventory processing.
    """
    print(
        f"{'Count':>15} |"
        f"{'Total Size':>16} |"
        f"{'Ver Size':>16} |"
        f"{'Del Size':>16} |"
        f"{'Avg Object':>16} |"
        " Folder\n",
        "-" * 110,
    )

    for folder, details in results.items():
        print(
            f"{details['Count']:>15} |",
            f"{convert_bytes(details['Size'], 'G'):>15} |",
            f"{convert_bytes(details['DelSize'], 'G'):>15} |",
            f"{convert_bytes(details['VerSize'], 'G'):>15} |",
            f"{convert_bytes(details['AvgObj'], 'K'):>15} |",
            urllib.parse.unquote(folder),
        )


def write_results(results: dict, out_file: str) -> None:
    """
    Writes the data from the inventory out to either local disk or S3.
    """
    print(f"Writing CSV results to {out_file}")

    csv_data = "Folder,Count,Size,DelSize,VerSize,AvgObject,Depth\n"
    for folder, details in results.items():
        row = (
            f"{urllib.parse.unquote(folder)},"
            f"{details['Count']},"
            f"{details['Size']},"
            f"{details['DelSize']},"
            f"{details['VerSize']},"
            f"{details['AvgObj']},"
            f"{details['Depth']}\n"
        )
        csv_data += row

    if out_file.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = parse_bucket_url(out_file)
        try:
            s3.put_object(Body=csv_data.encode(), Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as error:
            print("ERROR:", error, file=sys.stderr)
            sys.exit(1)
    else:
        with open(out_file, "w") as csv_file:
            csv_file.write(csv_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 Inventory Report")
    parser.add_argument(
        "-c", default="", dest="cache_dir", help="Folder name for caching data files"
    )
    parser.add_argument(
        "-d",
        type=int,
        dest="max_depth",
        help="The maximum depth folder to calculate sizing",
    )
    parser.add_argument(
        "-m",
        dest="manifest",
        required=True,
        help="The S3 Folder storing the manifest file",
    )
    parser.add_argument(
        "-o", dest="out_file", help="The local or S3 location to write the report"
    )
    args = parser.parse_args()

    try:
        main(args.manifest, args.max_depth, args.out_file, args.cache_dir)
    except KeyboardInterrupt:
        print("\nExiting", file=sys.stderr)
        sys.exit(1)
