#!/usr/bin/env python3
# encoding UTF-8

import requests as rq
from json import loads

# TODO:
# Add checking and constraints
class InsecureClient:
    _adress: str
    _user:   str

    def __init__(self, adress: str, port: str, user: str):
        self._adress = f"{adress}:{port}/webhdfs/v1"
        self._user   = user

    def _get(self, path: str, op: str) -> dict:
        content = b""
        with rq.get(f"{self._adress}{path}?user.name={self._user}&op={op}") as res:
            content = res.content
        return loads(content)
    
    def _delete(self, path: str, op: str, recursive: bool) -> dict:
        content = b""
        with rq.delete(f"{self._adress}{path}?user.name={self._user}&op={op}" + ("&recursive=true" if recursive else "")) as res:
            content = res.content
        return loads(content)
    
    def _post(self, path: str, op: str):
        content = b""
        with rq.post(f"{self._adress}{path}?user.name={self._user}&op={op}") as res:
            content = res.content
        return loads(content)

    def _put(self, path: str, op: str, args: str) -> dict:
        content = b""
        with rq.put(f"{self._adress}{path}?user.name={self._user}&op={op}" + args) as res:
            content = res.content
        return loads(content)

    def ls(self, path: str) -> dict:
        return self._get(path, "LISTSTATUS")
    
    def status(self, path: str) -> dict:
        return self._get(path, "GETFILESTATUS")
    
    def delete(self, path: str, recursive: bool = False):
        return self._delete(path, "DELETE", recursive)
    
    def mkdir(self, path: str, perm: int = 600):
        return self._put(path, "MKDIRS", f"&permission={perm}")
    
    def touch(self, path):
        return self._put(path, "CREATE")
    
    # This works the same way as GET & PUT
    def append(self, path: str, data: bytes):
        return self._post(path, "APPEND", data)
    
    # NOTE: GET & PUT do not work if you didn't configure endpoint for TEMPORARY_REDIRECT
    def open(self, path):
        return self._get(path, "OPEN")

# http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?user.name=<USER>&op=

# TODO:
# upload    : put       ????
# download  : get       ????
# append    : append    ????
# create    : touch     ????
# delete    : delete    DONE
# content   : ls        DONE
# makedirs  : mkdir     DONE
