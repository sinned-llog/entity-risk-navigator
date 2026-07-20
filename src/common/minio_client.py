import os
import json
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

    @classmethod
    def from_env(cls) -> "MinioClient":
        import os

        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            bucket=os.getenv("MINIO_BUCKET"),
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
    
    def get_object_bytes(self, object_key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        return response['Body'].read()
    
    def get_text_object(self, object_key: str, encoding: str = "utf-8-sig") -> str:
        content = self.get_object_bytes(object_key)
        
        return content.decode(encoding, errors='replace')
    
    def get_json_object(self, object_key: str) -> dict:
        text = self.get_text_object(object_key)

        return json.loads(text)
    
    def object_exists(self, object_key: str) -> bool:
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=object_key,
            )
            return True
        except ClientError:
            return False

    def list_objects(self, prefix: str) -> list:
        object_keys = []
        continuation_token = None

        while True:
            kwargs = {
                "Bucket": self.bucket,
                "Prefix": prefix,
            }

            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = self.client.list_objects_v2(**kwargs)

            for item in response.get("Contents", []):
                object_keys.append(item["Key"])

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        return object_keys