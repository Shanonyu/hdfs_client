from rest_hdfs import InsecureClient
from os import path as ospath

def split_with_quotes(string: str) -> list[str]:

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

def sanitize_remote_path(client: InsecureClient, path: str, arg: str) -> str | bool:

    if arg != "/": arg = arg.removesuffix("/")

    if arg.startswith("//"):
        path = "/"

    elif arg == "..":
        if path != "/":
            _path = "/".join(path.split("/")[:-2]) + "/"
            if _path == "": _path = "/"
            if client.exists(_path):
                path = _path
            else:
                return False

    elif arg.startswith("/"):
        _path = arg.removesuffix("/") + "/"
        if client.exists(_path):
            path = _path
        else:
            return False

    else:
        arg = arg.removeprefix(".").removeprefix("/")
        _path = path + arg + "/"
        if client.exists(_path):
            path = _path
        else:
            return False

    return path

def sanitize_local_path(arg: str, localpath: str) -> str | bool:

    if arg != "/": arg = arg.removesuffix("/")
    if arg.startswith("//"):
        localpath = "/"

    elif arg == "..":
        if localpath != "/":
            _path = "/".join(localpath.split("/")[:-2]) + "/"
            if _path == "": _path = "/"
            _ispath = ospath.lexists(_path)
            if _ispath:
                localpath = _path
            else:
                return False

    elif arg.startswith("/") or arg == "/":
        _path = ospath.lexists(arg)
        if _path:
            localpath = "/" if arg == "/" else arg + "/"
        else:
            return False

    else:
        arg = arg.removeprefix(".").removeprefix("/")
        _path = ospath.lexists(localpath + arg)
        if _path:
            localpath = localpath + arg + "/"
        else:
            return False

    return localpath

# Unused
def upload_progress(path, bytes):
    if bytes != -1:
        print("\rUploaded", bytes, "bytes...\t", end="")
    else:
        print("\rUploaded file:", path, "\t")

def download_progress(path, bytes):
    if bytes != -1:
        print("\rDownloaded", bytes, "bytes...\t", end="")
    else:
        print("\rDownloaded file:", path, "\t")
