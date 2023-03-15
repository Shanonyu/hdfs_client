#!/usr/bin/env python3
# encoding UTF-8

import requests as rq
from json import loads

# This is total bullshit
class InsecureClient:
    _adress: str
    _user:   str
    _api = "/webhdfs/v1"

    def __init__(self, adress: str, port: str, user: str):
        self._adress = f"{adress}:{port}"
        self._user   = user

    def _get(self, path: str, op: str) -> dict:
        adr = self._adress + self._api
        content = b""
        with rq.get(f"{adr}{path}?user.name={self._user}&op={op}") as res:
            content = res.content
        return loads(content)

    def _get_open(self, path: str, op: str) -> bytes:

        adr = self._adress + self._api
        content = b""
        link = ""

        with rq.get(f"{adr}{path}?user.name={self._user}&op={op}", allow_redirects=False) as res:
            link = res.headers.get("Location")

        with rq.get(link, allow_redirects=False) as res:
            content = res.content
        return content
    
    def _delete(self, path: str, op: str, recursive: bool) -> dict:
        adr = self._adress + self._api
        content = b""
        with rq.delete(f"{adr}{path}?user.name={self._user}&op={op}" 
                       + ("&recursive=true" if recursive else "")) as res:
            content = res.content
        return loads(content)
    
    def _post(self, path: str, op: str, data: bytes):
        adr = self._adress + self._api
        content = b""
        with rq.post(f"{adr}{path}?user.name={self._user}&op={op}") as res:
            content = res.content
        return loads(content)

    def _post_append(self, path: str, op: str, data: bytes):

        adr = self._adress + self._api
        content = b""
        link = ""

        with rq.post(f"{adr}{path}?user.name={self._user}&op={op}",
                     allow_redirects=False) as res:
            link = res.headers.get("Location")

        with rq.post(link, data=data, allow_redirects=False) as res:
            content = res.content
        return content

    def _put(self, path: str, op: str, args: str = None) -> dict:
        adr = self._adress + self._api
        content = b""
        with rq.put(f"{adr}{path}?user.name={self._user}&op={op}" + (args or ""), allow_redirects=False) as res:
            content = res.content
        return loads(content)

    def _put_touch(self, path: str, op: str, args: str = None, data: bytes = None) -> dict:

        adr = self._adress + self._api
        content = b""
        link = ""

        with rq.put(f"{adr}{path}?user.name={self._user}&op={op}" + (args if args else ""), allow_redirects=False) as res:
            link = res.headers.get("Location")

        with rq.put(link, data if data else None, allow_redirects=False) as res:
            content = res.content
        return content

    def ls(self, path: str) -> dict:
        return self._get(path, "LISTSTATUS")
    
    def status(self, path: str) -> dict:
        return self._get(path, "GETFILESTATUS")
    
    def delete(self, path: str, recursive: bool = False):
        return self._delete(path, "DELETE", recursive)
    
    def mkdir(self, path: str, perm: int = 600):
        return self._put(path, "MKDIRS", f"&permission={perm}")
    
    def touch(self, path):
        return self._put_touch(path, "CREATE")
    
    # This works the same way as GET & PUT
    def append(self, path: str, data: bytes):
        return self._post_append(path, "APPEND", data)
    
    # NOTE: GET & PUT do not work if you didn't configure endpoint for TEMPORARY_REDIRECT
    def open(self, path):
        return self._get_open(path, "OPEN")

    def download(self, path: str, filename: str):
        bstr = self.open(path)
        with open(filename, "x") as file:
            file.write(bstr)

    def upload(self, path, filename: str, data: bytes):
        return self._put_touch(path + "/" + filename, "CREATE", data=data)

