#!/usr/bin/env python3
# encoding UTF-8

import requests as rq
from json import loads

# TODO: Proper error handling
class Client:
    _adress: str
    _user:   str
    _url:    str

    _api = "/webhdfs/v1"
    _write_headers = {"Content-Type": "application/octet-stream"}

    def __init__(self, adress: str, port: str, user: str):
        self._adress = f"{adress}:{port}"
        self._user = user
        self._url = self._adress + Client._api

    def _get(self, path: str, op: str) -> bytes:

        url = self._url + path + f"?user.name={self._user}&op={op}"

        with rq.get(url) as res:
            content = res.content

        return content

    def _delete(self, path: str, op: str, recursive: bool) -> bytes:

        recursive = "&recursive=true" if recursive else ""
        url = self._url + path + f"?user.name={self._user}&op={op}"

        with rq.delete(url + recursive) as res:
            content = res.content

        return content

    def _post(self, path: str, op: str, data: bytes = None) -> bytes:

        headers = Client._write_headers if data else None
        url = self._url + path + f"?user.name={self._user}&op={op}"

        with rq.post(url, data, headers=headers) as res:
            content = res.content

        return content

    def _put(self, path: str, op: str, args: str = None, data: bytes = None) -> bytes:
        args = args or ""
        headers = Client._write_headers if data else None

        url = self._url + path + f"?ser.name={self._user}&op={op}" + args

        with rq.put(url, data, headers=headers) as res:
            content = res.content

        return content

    def exists(self, path: str) -> bool:
        status = self._get(path, "GETFILESTATUS")
        if status:
            status = loads(status)
            if status.get("RemoteException"):
                return False
            return True
        return False

    def ls(self, path: str) -> tuple[list, bool, str]:
        """-> (files, is_empty, error)"""
        files = self._get(path, "LISTSTATUS")
        if files:
            files = loads(files)
            result = files.get("FileStatuses").get("FileStatus")
            if result:
                return (result, False, None)
            return (None, True, None)
        return (None, False, files)

    def status(self, path: str) -> dict:
        return loads(self._get(path, "GETFILESTATUS"))

    def mkdir(self, path: str, perm: int = 600) -> bool:
        return loads(self._put(path, "MKDIRS", f"&permission={perm}")).get("boolean")

    def touch(self, path: str, filename: str) -> tuple[bool, str]:
        """-> (success, error)"""
        result = self._put(path + filename, "CREATE")
        err = loads(result) if result else None
        if err:
            err: str = err.get("RemoteException").get("message")
            err = err.split("\n", 1)[0]
        return (not result or None, err)

    def append(self, path: str, filename: str, data: bytes) -> tuple[bool, str]:
        """-> (success, error)"""
        result = self._post(path + filename, "APPEND", data)
        err = loads(result) if result else None
        if err:
            err = err.get("RemoteException").get("message")
        return (not result or None, err)

    def open(self, path: str, filename: str) -> tuple[bytes, str]:
        """-> (bytes, error)"""
        result = self._get(path + filename, "OPEN")
        try:
            return (None, loads(result).get("RemoteException").get("message"))
        except:
            return (result, None)

    # TODO: Read a chunk at a time
    def download(self, path: str, filename: str, localpath: str, encoding: str = "utf-8") -> tuple[bool, str]:
        """-> (success, error)"""
        bstr, err = self.open(path, filename)
        if err:
            return (False, err)
        with open(localpath + filename, "x") as file:
            file.write(bstr.decode(encoding))
            return (True, None)

    # TODO: Upload a chunk at a time
    def upload(self, path, filename: str, data: bytes) -> tuple[bool, str]:
        """-> (success, error)"""
        result = self._put(path + filename, "CREATE", data=data)
        err = result if result else None
        if err:
            err = err.get("RemoteException").get("message")
        return (not result or None, err)

    def delete(self, path: str, filename: str,  recursive: bool = False) -> bool:
        return loads(self._delete(path + filename, "DELETE", recursive)).get("boolean")
