# digitalocean-cdn-backup
The script downloads files from DigitalOcean Spaces using boto3. It connects to a specified endpoint, lists files in a bucket, and downloads them to the local disk, maintaining the directory structure. It tracks download progress and handles errors, such as missing credentials or client API issues.
