#!/usr/bin/env python

from copy import copy
from datetime import datetime
import gzip
from hashlib import md5
from json import loads
import os
import sys
import urllib.parse

from pyarrow import BufferReader, csv, orc, parquet
from boto3 import client
import botocore.exceptions

file_cache = './cache'
s3 = client('s3')


def cli():
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
                    folder,
                    details["Count"],
                    details["Size"],
                    details["DelSize"],
                    details["VerSize"],
                    details["AvgObj"],
                ))
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
        manifest_json = s3.get_object(Bucket=bucket,
                                      Key=json_key).get('Body').read()
        manifest_checksum = s3.get_object(Bucket=bucket,
                                          Key=checksum_key).get('Body').read()
    except botocore.exceptions.ClientError as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)

    if md5(manifest_json).hexdigest() != manifest_checksum.decode().rstrip("\n"):
        raise SystemError('The manifest failed the MD5 Check')

    return loads(manifest_json)


def process_investory(manifest, max_depth) -> dict:
    template = {"Count": 0, "DelSize": 0, "Size": 0, "VerSize": 0}
    folders = {"/": copy(template)}
    object_count = 0

    if not os.path.isdir(file_cache):
        os.mkdir(file_cache)

    print("Processing Inventory")
    start = datetime.now()

    for file in manifest["files"]:
        inventory_bucket = manifest['destinationBucket'].split(":::")[1]
        local_file = file_cache + file['key'][file['key'].rindex("/"):]

        if os.path.isfile(local_file):
            print('Local:', local_file)
            with open(local_file, 'rb') as file:
                data = file.read()
        else:
            print('S3:', file['key'])
            file_object = s3.get_object(Bucket=inventory_bucket,
                                        Key=file["key"])
            data = file_object.get('Body').read()

            if md5(data).hexdigest() != file['MD5checksum']:
                raise SystemError('The inventory file failed the MD5 Check')

            with open(local_file, 'wb') as file:
                file.write(data)

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
            read_options = csv.ReadOptions(column_names=csv_names)
            table = csv.read_csv(BufferReader(gzip.decompress(data)),
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

    duration = datetime.now() - start
    print("Processed %d objects in %d seconds\n" % (object_count,
                                                    duration.seconds))

    for folder, details in folders.items():
        folders[folder]["AvgObj"] = round(details["Size"] / details["Count"])
        folders[folder] = urllib.parse.unquote(folders[folder])

    return folders


def print_results(results: dict) -> None:
    print(f"{'Count':>15} |"
          f"{'Total Size':>16} |"
          f"{'Ver Size':>16} |"
          f"{'Del Size':>16} |"
          f"{'Avg Object':>16} |"
          " Folder\n", "-" * 120)

    for folder, details in results.items():
        print(f"{details['Count']:>15} |",
              f"{convert_bytes(details['Size'], 'G'):>15} |",
              f"{convert_bytes(details['DelSize'], 'G'):>15} |",
              f"{convert_bytes(details['VerSize'], 'G'):>15} |",
              f"{convert_bytes(details['AvgObj'], 'K'):>15} |",
              folder)


def parse_bucket_name(name):
    name = name.lstrip("s3://")
    return (name[:name.index("/")], name[name.index("/"):].lstrip("/"))


if __name__ == "__main__":
    sys.exit(cli())
