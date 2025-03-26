import os
import boto3
import time
import sys
from botocore.exceptions import NoCredentialsError, ClientError

# Configuration
CDS_ENDPOINT = "https://fra1.digitaloceanspaces.com"  # Change to the appropriate region
ACCESS_KEY = ""
SECRET_KEY = ""
BUCKET_NAME = ""

# Local path for file storage
DOWNLOAD_PATH = r"L:\backup"

# Maximum full path length in Windows
MAX_PATH_LENGTH = 260

# DigitalOcean Spaces connection configuration
s3 = boto3.client(
    "s3",
    endpoint_url=CDS_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=boto3.session.Config(signature_version="s3v4"),
)

def list_files():
    """Retrieves a list of files from DigitalOcean Spaces."""
    files = []
    total_size = 0
    try:
        kwargs = {"Bucket": BUCKET_NAME}
        while True:
            response = s3.list_objects_v2(**kwargs)
            if "Contents" in response:
                files.extend(response["Contents"])
                total_size += sum(item["Size"] for item in response["Contents"])
            if not response.get("IsTruncated"):  # End of list
                break
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
    except ClientError as e:
        print(f"API Error: {e}")
    return files, total_size / (1024 * 1024)  # Convert bytes to MB

def create_directories(file_key):
    """Creates directories recursively based on the file key."""
    dir_path = os.path.normpath(os.path.join(DOWNLOAD_PATH, os.path.dirname(file_key.lstrip("/"))))
    if len(dir_path) > MAX_PATH_LENGTH:
        return None
    os.makedirs(dir_path, exist_ok=True)
    return dir_path

def print_progress(downloaded, total_size, start_time):
    """Displays download progress in the terminal in green color."""
    percent = (downloaded / total_size) * 100 if total_size else 0
    elapsed_time = time.time() - start_time
    speed = downloaded / elapsed_time if elapsed_time > 0 else 0  # MB/s
    remaining_time = (total_size - downloaded) / speed if speed > 0 else 0  # Seconds

    hours = int(remaining_time // 3600)
    minutes = int((remaining_time % 3600) // 60)

    progress_line = f"\033[92mProgress: {percent:.2f}% | Downloaded: {downloaded:.2f}/{total_size:.2f} MB | Estimated time: {hours}h {minutes}m\033[0m"
    sys.stdout.write("\r" + progress_line)
    sys.stdout.flush()

def download_file(file_key, total_size_downloaded):
    """Downloads a single file from DigitalOcean Spaces."""
    local_path = os.path.join(DOWNLOAD_PATH, file_key.lstrip("/"))

    if len(local_path) > MAX_PATH_LENGTH:
        return 0, total_size_downloaded  # Skipped file

    # Check if the file already exists
    if os.path.exists(local_path):
        # Return the size of the existing file and do not change the total downloaded size
        file_size = os.path.getsize(local_path) / (1024 * 1024)  # in MB
        total_size_downloaded += file_size  # Add size to downloaded
        return file_size, total_size_downloaded  # Skip time for existing files

    create_directories(file_key)

    try:
        s3.download_file(BUCKET_NAME, file_key, local_path)
        file_size = os.path.getsize(local_path) / (1024 * 1024)  # in MB
        total_size_downloaded += file_size  # Add size to downloaded
        return file_size, total_size_downloaded
    except ClientError:
        return 0, total_size_downloaded  # If error, treat as 0 MB

def main():
    print("Downloading file list...")
    files, total_size = list_files()

    if not files:
        print("No files to download.")
        return

    downloaded_size = 0
    start_time = time.time()

    for file in files:
        file_key = file["Key"]
        file_size, downloaded_size = download_file(file_key, downloaded_size)
        print_progress(downloaded_size, total_size, start_time)

    print("\nDownload finished.")

if __name__ == "__main__":
    main()
