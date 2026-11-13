"""Create the four data-zone buckets without replacing existing data."""

import os

from minio import Minio
from minio.error import S3Error

BUCKETS = ("raw", "staging", "curated", "rejected")


def main() -> None:
    """Ensure every bucket exists; propagate authentication and storage errors."""
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,  # Internal, isolated local Compose network.
    )
    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            try:
                client.make_bucket(bucket)
            except S3Error as exc:
                # Another initializer may have created it after the existence check.
                if exc.code != "BucketAlreadyOwnedByYou":
                    raise
        print(f"Bucket ready: {bucket}", flush=True)


if __name__ == "__main__":
    main()
