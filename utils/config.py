"""
Environment-based configuration for Poet App backend.
All sensitive data loaded from environment variables.
"""

import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class that loads all settings from environment variables."""

    # Database
    psql_uri = os.getenv("DATABASE_URL")

    # Database Connection Pool Configuration
    db_pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
    db_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    db_pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    db_pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    db_pool_pre_ping = os.getenv("DB_POOL_PRE_PING", "true").lower() == "true"

    # Redis
    redis_uri = os.getenv("REDIS_URL")
    redis_password = os.getenv("REDIS_PASSWORD", None)

    # Storage paths
    profile_dir = os.getenv("PROFILE_DIR", "/tmp/profiles")
    upload_dir = os.getenv("UPLOAD_DIR", "/tmp/uploads")

    # External URLs
    image_url = os.getenv("IMAGE_URL")
    api_uri = os.getenv("API_URI")
    ocr_url = os.getenv("OCR_URL")

    # S3/MinIO
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_access_key_secret = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_bucket_name = os.getenv("S3_BUCKET_NAME", "poet-upload")
    s3_image_bucket_name = os.getenv("S3_IMAGE_BUCKET_NAME", "image")

    # Application settings
    is_dev = os.getenv("DEV_MODE", "false").lower() == "true"
    debug = os.getenv("DEBUG", "false").lower() == "true"

    # Milvus
    milvus_uri = os.getenv("MILVUS_URI")
    milvus_token = os.getenv("MILVUS_TOKEN")
    milvus_db = os.getenv("MILVUS_DB")

    # Embedding Configuration
    embedding_url_sparse = os.getenv("EMBEDDING_URL_SPARSE")
    embedding_model = os.getenv("EMBEDDING_MODEL")
    embedding_url = os.getenv("EMBEDDING_URL")
    embedding_key = os.getenv("EMBEDDING_KEY")

    # Logging
    log_level = os.getenv("LOG_LEVEL", "INFO")


# Global config instance
config = Config()
