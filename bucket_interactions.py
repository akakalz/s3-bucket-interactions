from typing import Generator, List
import boto3
import smart_open


def list_bucket_objects(bucket: str) -> List[str]:
    session = boto3.Session()
    s3 = session.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    return [
        item.key
        for item in s3_bucket.objects.all()
    ]


def iterate_s3_file(bucket: str, key: str) -> Generator[str, None, None]:
    while key.startswith("/"):
        key = key[1:]
    with smart_open.open(f"s3://{bucket}/{key}") as in_file:
        for line in in_file:
            yield line
