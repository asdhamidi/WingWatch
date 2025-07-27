#!/bin/sh
set -e

# Wait for MinIO to be ready
until mc alias set minio http://minio:9000 minio_admin minio_password; do
  echo "Waiting for MinIO..."
  sleep 1
done


echo "MinIO initialized"