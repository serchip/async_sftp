#!/usr/bin/python

import os
import pytest
import tempfile
import logging

from ..sftp_storage import is_py3
from ..sftp_storage import SFTPClientException

if is_py3():
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO

logger = logging.getLogger('sftp_storage.test')
SELECTEL_CONTAINER_PATH = ""
from offerBuilder.core.settings import CLOUD_IMG

@pytest.mark.asyncio
async def test_put(resource):
    client = resource
    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    if result:
        return True

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test_rules.pdf')
    with open(file_path, 'rb') as f:
        result = await client.put(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf", f.read())
        assert result == True
        f.close()

@pytest.mark.asyncio
async def test_delete(resource):
    print('test_delete')
    client = resource
    await test_put(client)
    result = await client.remove(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == True
    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == False

    await test_put(client)
    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == True
    result = await client.remove(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf", force=True, remove_last_dir=True)

    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == False
    dir_name = os.path.dirname(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    result = await client.exist_dir(dir_name)
    assert  result == False

@pytest.mark.asyncio
async def test_exist(resource):
    print('test_exist')
    client = resource
    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/not_exist_file.pdf")
    assert result == False

    await test_put(client)

    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == True

    await test_delete(client)
    result = await client.exist(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}cm/test/test_rules.pdf")
    assert result == False

@pytest.mark.asyncio
async def test_list(resource):
    print('test_list')
    client = resource
    result = await client.list(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}")
    assert len(result) > 100


@pytest.mark.asyncio
async def test_get_content(resource):
    print('test_get_content')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test_rules_from_server.pdf')

    client = resource
    await test_put(resource)

    def get_callback(size, file_size):
        assert os.path.isfile(file_path) == True

    await client.get(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}/cm/test/test_rules.pdf", file_path, get_callback)

    os.unlink(file_path)

    try:
        await client.get(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}/not_exist_file.pdf", file_path, get_callback)
    except IOError as e:
        assert str(e).rfind("No such file") > 0


@pytest.mark.asyncio
async def test_get_steam(resource):
    print('test_get_steam')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'test_rules_from_server.pdf')

    def get_callback(size, file_size):
        assert os.path.isfile(file_path) == True

    client = resource
    await test_put(resource)

    with open(file_path, "wb") as fl:
        result = await client.get_steam(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}/cm/test/test_rules.pdf", fl, get_callback)

    assert type(result) == int
    os.unlink(file_path)
    try:
        with open(file_path, "wb") as fl:
            await client.get_steam(f"{CLOUD_IMG['cloud']['PDF_HOST_PATH']}/not_exist_file.pdf", fl)
    except IOError as e:
        assert str(e).rfind("No such file") > 0


