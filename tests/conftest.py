import pytest
from urllib.parse import urlparse

from ..sftp_storage import SFTPClientApi
from ..settings import CREDENTIAL_SFTP
from offerBuilder.core.settings import CLOUD_IMG

@pytest.fixture()
async def resource():
    target = urlparse(CLOUD_IMG['cloud']['SFTP_CONNECT_URL'])

    client = SFTPClientApi(host=target.hostname,
                           username=target.username,
                           password='',
                           use_known_hosts=False,
                           max_retry=CREDENTIAL_SFTP['MAX_RETRY'], retry_delay=CREDENTIAL_SFTP['RETRY_DELAY']
                           )
    yield client
