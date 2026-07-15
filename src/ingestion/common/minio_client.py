import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class MinioClient:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def ensure_bucket_exists(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            print(f"Bucket exists: {self.bucket}")
        except ClientError:
            print(f"Creating bucket: {self.bucket}")
            self.client.create_bucket(Bucket=self.bucket)

    def upload_file(
        self,
        object_key: str,
        local_file_path: str,
        content_type: str,
    ) -> None:
        self.client.upload_file(
            Filename=local_file_path,
            Bucket=self.bucket,
            Key=object_key,
            ExtraArgs={"ContentType": content_type},
        )

    def upload_bytes(
        self,
        object_key: str,
        content: bytes,
        content_type: str,
    ) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=content,
            ContentType=content_type,
        )