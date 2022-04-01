import pytest
import boto3
import moto
from os import getenv, path
from bucket_interactions import(
    iterate_s3_file,
    list_bucket_objects,
)


@pytest.fixture
def moto_creds(monkeypatch):
    """
    sets some needed items for moto to work

    us-east-1 is the default as it is the lone region that
    works with all moto functionality
    """
    for k, v in {
        "AWS_ACCESS_KEY": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        if not getenv(k):
            monkeypatch.setenv(k, v)


@pytest.fixture
def test_bucket():
    return "test_bucket"


@pytest.fixture
def test_prefix():
    return "path/to/prefix/"


@pytest.fixture
def populate_test_bucket(moto_creds, test_bucket, test_prefix):
    """
    create a bucket and place files into it
    """
    files = ["file_1.txt", "file_2.txt"]
    with moto.mock_s3():
        s3_res = boto3.resource("s3")
        s3_res.create_bucket(Bucket=test_bucket)
        s3_client = boto3.client("s3")
        for file in files:
            key_name = path.join(test_prefix, file)
            s3_client.put_object(
                Bucket=test_bucket,
                Key=key_name,
                Body="line1\nline2\nline3\n",  # simple text
            )
        yield s3_res


def test_iterate_s3_file(populate_test_bucket, test_bucket, test_prefix):
    # arrange
    key = f"{test_prefix}file_1.txt"
    expected = ["line1\n", "line2\n", "line3\n"]
    # act
    actual = list(iterate_s3_file(test_bucket, key))
    # assert
    assert actual == expected


def test_list_bucket_objects(populate_test_bucket, test_bucket, test_prefix):
    # arrange
    expected = [f"{test_prefix}{file}" for file in ["file_1.txt", "file_2.txt"]]
    # act
    actual = list_bucket_objects(test_bucket)
    # assert
    assert actual == expected
