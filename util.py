#!/usr/bin/env python3
# encoding UTF-8

from rest_hdfs import Client
from os import path as ospath

def split_with_quotes(string: str) -> list[str]:
    """hello world \"wait i meant\" sailor -> [\"hello\", \"world\", \"wait i meant\", \"sailor\"]"""

    string = string.strip()
    tmp = ""
    result = []
    l = len(string)
    i = 0

    while (i < l):
        if string[i] == " ":
            i += 1
            if string[i] == '"':
                i += 1
                while (i < l and string[i] != '"'):
                    tmp += string[i]
                    i += 1
                result.append(tmp)
                tmp = ""
                i += 1
            else:
                while (i < l and string[i] != " "):
                    tmp += string[i]
                    i += 1
                result.append(tmp)
                tmp = ""
        else:
            while (i < l and string[i] != " "):
                tmp += string[i]
                i += 1
            result.append(tmp)
            tmp = ""
    return result

# TODO: This could be Client class' method (classes >:c)
def sanitize_remote_path(current_path: str, new_path: str, client: Client) -> str | None:
    """Returns new path if remote path exists. Returns `None` if remote path is not valid."""

    if new_path != "/": new_path = new_path.removesuffix("/")

    if new_path.startswith("//"):
        current_path = "/"

    # Go up one level ..
    elif new_path == "..":
        if current_path != "/":
            _path = "/".join(current_path.split("/")[:-2]) + "/"
            if _path == "": _path = "/"
            if client.exists(_path):
                current_path = _path
            else:
                return None

    # Absolute path /
    elif new_path.startswith("/"):
        _path = new_path.removesuffix("/") + "/"
        if client.exists(_path):
            current_path = _path
        else:
            return None

    # Relative path ./ or no prefix
    else:
        new_path = new_path.removeprefix(".").removeprefix("/")
        _path = current_path + new_path + "/"
        if client.exists(_path):
            current_path = _path
        else:
            return None

    return current_path

def sanitize_local_path(current_path: str, new_path: str) -> str | None:
    """Returns new path if local path exists. Returns `None` if path is not valid."""
    if new_path != "/": new_path = new_path.removesuffix("/")
    if new_path.startswith("//"):
        current_path = "/"

    # Go up one level ..
    elif new_path == "..":
        if current_path != "/":
            _path = "/".join(current_path.split("/")[:-2]) + "/"
            if _path == "": _path = "/"
            _ispath = ospath.lexists(_path)
            if _ispath:
                current_path = _path
            else:
                return None

    # Absolute path /
    elif new_path.startswith("/") or new_path == "/":
        _path = ospath.lexists(new_path)
        if _path:
            current_path = "/" if new_path == "/" else new_path + "/"
        else:
            return None

    # Relative path ./ or no prefix
    else:
        new_path = new_path.removeprefix(".").removeprefix("/")
        _path = ospath.lexists(current_path + new_path)
        if _path:
            current_path = current_path + new_path + "/"
        else:
            return None

    return current_path
