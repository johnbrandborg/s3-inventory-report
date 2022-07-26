# S3 Inventory Report
Processes S3 Inventory Manifests and generates a report about the folder size and object size average

## Setup

First configure S3 to create inventory for your bucket/s.

https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html

Once completed install the requirements file and use the CLI. Not required if
running on any platform with Pandas, like Airflow.  This script uses PyArrow
for loading the CSV, ORC and Parquet files.

```shell
pip install -r requirements.txt
```

## Usage

This tool is a simple CLI script. By default the information is displayed on the screen, but if
a out file is set a CSV file is created either locally or on S3.

```shell
./s3-inventory-report.py \
    -m s3://inventory-bucket/production-bucket/Daily/2022-07-24T00-00Z/ \
    -o s3://archive-bucket/2022-07-24.csv \
    -d 3 \
    -c ./cache/
```

The CSV will have a header with the following fields:
 1. Folder - Name of folder in the S3 Bucket
 2. Count - Number of Objects in the folder
 3. Size - Total size of the folder.  Current, NonCurrent, and deletion marked
 4. DelSize - Total size of the Objects with deletion marker
 5. VerSize - Total size of the Objects that are a noncurrent version
 6. AvgObject - Average Object size of the objects in the Folder
