from datetime import datetime
from time import sleep
import sys
import os
from threading import Thread

from boto3 import client

inventory_file = "inventory.log"
monitor = True


def convert_bytes(size, unit=None):
    if unit == "K":
        return str(round(size / 1024, 3)) + ' KB'
    elif unit == "M":
        return str(round(size / (1024 * 1024), 3)) + ' MB'
    elif unit == "G":
        return str(round(size / (1024 * 1024 * 1024), 3)) + ' GB'
    else:
        return str(size) + ' Bytes'

def create_inventory(bucket_name, prefix):
    global monitor
    object_count = 0

    def progress():
        global monitor
        reference = 0
        while monitor:
            if offset:= object_count - reference:
                print("." * int(offset / 10000), end="", flush=True, file=sys.stderr)
            reference = object_count
            sleep(5)

    s3 = client('s3')
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    if not os.path.exists(inventory_file):
        print("Creating Inventory", file=sys.stderr)
        Thread(target=progress).start()
        start = datetime.now()
        file = open(inventory_file, "w")
        keys = []

        for page in page_iterator:
            for key in page['Contents']:
                keys.append('"%s","%d"\n' % (key['Key'], key['Size']))
                object_count += 1
            
            if len(keys) >= 10000:
                file.writelines(keys)
                keys.clear()

        file.writelines(keys)
        file.close()
        duration, monitor  = (datetime.now() - start, False)
        print("\nScanned %d objects in %d minutes" % (object_count,
                                                      duration.seconds / 60),
             file=sys.stderr)

def process_investory(max_depth):
    top_level_folders = {"/": 0}

    print("Processing Inventory", file=sys.stderr)
    with open(inventory_file) as f:
        for line in f.readlines():
            fields = line.lstrip("\"").rstrip("\"\n").split("\",\"")
            folder, size = (fields[0], int(fields[1]))

            folder_count = folder.count("/")
            depth = folder_count if max_depth == None or \
                folder_count <= max_depth else max_depth

            for index in range(1, depth + 1):
                entry = "/" + "/".join(folder.split("/")[:index]) + "/"

                top_level_folders["/"] += size
                try:
                    top_level_folders[entry] += size
                except KeyError:
                    top_level_folders[entry] = size

    print("-" * 40, file=sys.stderr)
    for folder, size in top_level_folders.items():
        print(f"{convert_bytes(size, 'G'):>15} |", folder)

def parse_bucket_name(name):
    break_down = name.lstrip("s3://").split("/")
    return (break_down[0], "/".join(break_down[1:]))

def cli():
    global monitor
    bucket_name = None
    max_depth = None

    if len(sys.argv) == 1:
        print("S3 Folder Size Report\n" + "-" * 40 + "\n",
              "-b <bucket_name>/<prefix>\n",
              "-d <max_depth>  Optional")
        return 1

    for index, argv in enumerate(sys.argv):
        if "-b" == argv:
            bucket_name, prefix = parse_bucket_name(sys.argv[index + 1])
        if "-d" == argv:
            try:
                max_depth = int(sys.argv[index + 1])
            except ValueError:
                print("ERROR: Maximum depth needs to be an integer")
                return 1

    if not bucket_name:
        print("ERROR: A bucket name must be supplied")
        return 1

    try:
        create_inventory(bucket_name, prefix)
        process_investory(max_depth)
    except KeyboardInterrupt:
        print("\nExiting", file=sys.stderr)
        monitor = False
        return 1

if __name__ == "__main__":
    sys.exit(cli())

