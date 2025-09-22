import s3fs

from utils.config import config

fs = s3fs.S3FileSystem(
    key=config.s3_access_key_id,
    secret=config.s3_access_key_secret,
    client_kwargs={
        "endpoint_url": config.s3_endpoint_url,
        "region_name": "None",
    },
    config_kwargs={"s3": {"addressing_style": "path"}},
    use_ssl=False,
)
