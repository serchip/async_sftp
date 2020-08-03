import os
CREDENTIAL_SFTP = {
    "MAX_RETRY": os.environ.get('CDN_CLOUD_MAX_RETRY', 2),
    "RETRY_DELAY": os.environ.get('CDN_CLOUD_RETRY_DELAY', 2),
}
