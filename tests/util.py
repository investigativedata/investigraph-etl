import boto3

from tests.conftest import FIXTURES_PATH


def setup_s3_bucket(with_content: bool | None = False):
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="investigraph")
    if with_content:
        client = boto3.client("s3")
        for f in ("all-authorities.csv", "ec-meetings.xlsx"):
            client.upload_file(FIXTURES_PATH / f, "investigraph", f)
