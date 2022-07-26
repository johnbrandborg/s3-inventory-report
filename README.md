# S3 Inventory Report
Processes S3 Inventory Manifests and generates a report about the folder size and object size average


## Setup

First configure S3 to create inventory for your bucket/s.

https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html

Once done you can run `s3-folder-size-report` and display the data or create a csv file.

```shell
./s3-folder-size-report.py -m s3://inventory-bucket/production-bucket/EntireBucketDaily/2022-07-24T00-00Z/ -d 3
```
