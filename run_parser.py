from src.rvtools_parser import chunk_and_write
import argparse
import json
import os
import sys


def main():
    parser = argparse.ArgumentParser(description="Run RVTools chunker")
    parser.add_argument("--input", required=True, help="path to RVTools XLSX file")
    parser.add_argument("--sheet", required=True, help="sheet name to parse")
    parser.add_argument("--out", required=True, help="output dir for chunk files")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--upload-s3", action="store_true", help="upload chunks to S3/MinIO")
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"), help="S3 endpoint")
    parser.add_argument("--s3-access-key", default=os.environ.get("S3_ACCESS_KEY"), help="S3 access key")
    parser.add_argument("--s3-secret-key", default=os.environ.get("S3_SECRET_KEY"), help="S3 secret key")
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"), help="S3 bucket")
    parser.add_argument("--ingest-id", default=os.environ.get("INGEST_ID"), help="ingest ID")
    args = parser.parse_args()

    result = chunk_and_write(
        args.input, args.sheet, args.out, args.chunk_size,
        upload_s3=args.upload_s3,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        s3_bucket=args.s3_bucket,
        ingest_id=args.ingest_id
    )
    print(f"Manifest: {result['manifest_path']}", file=sys.stderr)
    print(
        f"Chunks: {result['manifest']['chunk_count']}, Rows: {result['manifest']['total_rows']}",
        file=sys.stderr,
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
