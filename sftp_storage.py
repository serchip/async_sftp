# coding=utf-8
from __future__ import unicode_literals

import asyncio, aiohttp
import sys, os
import pysftp

from functools import wraps

from typing import Dict, List, Any, Tuple

def is_py3():
    return sys.version_info.major == 3

if is_py3():
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO


class SFTPClientException(Exception):

    def __init__(self, message, *args, **kwargs):
        message = "SFTPClient: {}".format(message)
        super(SFTPClientException, self).__init__(message)


class SFTPClientApi(object):
    """Класс для работы с SFTP

        На текущий момент с помощью API можно выполнять следующие операции:
            * получать содержимое директории;
            * загружать файлы в директорию;
            * удалять файлы ;
    """

    def __init__(self, host, username, password, max_retry, retry_delay, use_known_hosts=False):
        self._host = host
        self._username = username
        self._password = password
        self._cnopts = pysftp.CnOpts()
        if not use_known_hosts:
            self._cnopts.hostkeys = None
        self.max_retry = max_retry
        self.retry_delay = retry_delay

        self._session = None

    def update_expired(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            if not self._session:
                await self.authenticate()
            try:
                response = await fn(self, *args, **kwargs)
            except (SFTPClientException) as e:
                if not self._session:
                    await self.authenticate()
                    response = await fn(self, *args, **kwargs)
                else:
                    raise e
            await self.close_session()
            return response

        return wrapper

    def attempts(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            if self.max_retry is not None:
                retries = self.max_retry
                while retries > 1:
                    try:
                        return await fn(self, *args, **kwargs)
                    except (SFTPClientException):
                        retries -= 1
                        await asyncio.sleep(self.retry_delay)
                response = await fn(self, *args, **kwargs)
                return response
        return wrapper

    async def close_session(self):
        self._session.close()
        self._session = None

    async def authenticate(self):
        if not self._username or not self._password:
            raise SFTPClientException("Not set user or password")

        self._session = pysftp.Connection(
            host=self._host,
            username=self._username,
            password=self._password,
            cnopts=self._cnopts,
        )

    @attempts
    @update_expired
    async def get(self, path: str, localpath, callback=None) -> None:
        """Получение файлов с сервиса и положить в локальную папку

            Args:
                path: пусть до файла
                localpath: локальная папка
                callback: вызвать функцию после выполнения
            Return:
                ничего не возвращает (используем callback)
        """
        self._session.get(path, localpath, callback)

    @attempts
    @update_expired
    async def get_steam(self, path: str, localpath, callback=None) -> Any:
        """Получение файла из контейнера в виде open(file)

            Args:
                path: пусть до файла
            Return:
                объект файла
        """
        return self._session.getfo(path, localpath, callback)

    @attempts
    @update_expired
    async def remove(self, path: str, force: bool = False, remove_last_dir: bool = False) -> bool:
        """Удаление файла на сервере

            Args:
                path: пусть до файла
                force: пропустить генерацию exception в случае ошибки (отсутствие файла..)
            Return:
                Результат удаления
        """
        try:
            self._session.remove(path)
        except Exception as e:
            if not force:
                await self.close_session()
                raise SFTPClientException("Error remove file {}: {}".format(path, str(e)))
        try:
            if remove_last_dir:
                dir_name = os.path.dirname(path)
                self._session.rmdir(dir_name)
        except Exception as e:
            if not force:
                await self.close_session()
                raise SFTPClientException("Error remove file {}: {}".format(dir_name, str(e)))
        return True

    @attempts
    @update_expired
    async def put(self, path_cloud: str, content: bytes) -> bool:
        """Загрузка файла в директорию

            Args:
                path_cloud: пусть до файла
            Return:
                True/except
            """
        self.exist_or_make_dir(path_cloud)
        try:
            if is_py3():
                buffer = BytesIO(content)
            else:
                buffer = StringIO(content)
            r = self._session.putfo(flo=buffer, remotepath=path_cloud)
        except Exception as e:
            await self.close_session()
            raise SFTPClientException("Error put file {}: {}".format(path_cloud, str(e)))
        return True

    @attempts
    @update_expired
    async def exist(self, path: str) -> bool:
        """Проверка существования файла на сервере

            Args:
                path: пусть до файла
            Return:
                True/False
        """
        return self._session.isfile(path)

    @attempts
    @update_expired
    async def exist_dir(self, path: str) -> bool:
        """Проверка существования директории на сервере

            Args:
                path: пусть до папки
            Return:
                True/False
        """
        return self._session.isdir(path)

    def exist_or_make_dir(self, path: str) -> None:
        """Проверка и создание директорий"""
        dir_name = os.path.dirname(path)
        if not self._session.isdir(dir_name):
            self._session.makedirs(dir_name)
        return

    @attempts
    @update_expired
    async def list(self, path: str) -> List:
        """Получение списка файлов на сервере

            Args:
                path: путь до папки
            Return:
                Возвращает список файлов, находящихся в указанном папке.
            """
        return self._session.listdir(path)
