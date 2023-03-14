#!/usr/bin/env python3

if __name__ != "__main__": exit(0)

from sys  import argv
from hdfs import InsecureClient
from os   import listdir
from os   import path as ospath

def sanitize_remote_path(path, arg: str) -> str | bool:

    if arg != "/": arg = arg.removesuffix("/")

    if arg.startswith("//"):
        path = "/"

    elif arg == "..":
        if path != "/":
            _path = "/".join(path.split("/")[:-2]) + "/"
            if _path == "": _path = "/"
            if client.content(_path, False) is not None:
                path = _path
            else:
                return False

    elif arg.startswith("/"):
        _path = arg.removesuffix("/") + "/"
        if client.content(_path, False) is not None:
            path = _path
        else:
            return False

    else:
        arg = arg.removeprefix(".").removeprefix("/")
        _path = path + arg + "/"
        if client.content(_path, False) is not None:
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

if len(argv) < 4:
    print(f"USAGE: {argv[0]} <address> <port> <user>")
    exit(0)

address = argv[1]
if not address.startswith("http"):
    address = "http://" + address
port   = argv[2]
user   = argv[3]

print("Connecting:", address + ":" + port, "as", user + "...")
client = InsecureClient(f"{address}:{port}", user)

try:
    client.list("/", True)
except Exception as e:
    print("Could not connect to HDFS:")
    print(e)
    print("Check if address and port are valid.")
    exit(1)

quit = False
path = "/"
localpath = ospath.expanduser("~") + "/"

while (not quit):

    print(f"[loc: {localpath}]")
    print(f"[rem: {path}]")
    print(user, "# ", end="")
    cmd = (input())
    args = cmd.split()
    l = len(args)

    match args[0]:
        case "append":
            if l < 3:
                print("USAGE: append <local file> <remote file>")
                print("Appends contents from local file to remote file.")
            else:
                _localpath = localpath + args[1]
                _path = path + args[2]
                if client.content(_path, False) is not None:
                    if ospath.lexists(_localpath):
                        # TODO: Read file 1024 bytes at a time to allow for bigger files
                        try:
                            data = b""
                            with client.read(_path) as remote_file:
                                with open(_localpath) as local_file:
                                    data = remote_file.read() + local_file.read().encode("utf-8")
                            with client.write(_path, overwrite=True) as writer:
                                writer.write(data)
                        except Exception as e:
                            print("Could not read files:")
                            print(e)
                    else:
                        print("Could not find local file specified.")
                else:
                    print("Could not find remote file specified.")

        case "put":
            if l < 2:
                print("USAGE: put <file>")
                print("Puts local file in remote directory.")
            else:
                if ospath.lexists(localpath + args[1]):
                    file = client.upload(path, localpath + args[1], progress=upload_progress)
                else:
                    print("Could not find file specified.")

        case "get":
            if l < 2:
                print("USAGE: get <file>")
                print("Puts remote file in local directory.")
            else:
                _path = path + args[1]
                if client.content(_path, False) is not None:
                    file = client.download(path + args[1], localpath, progress=download_progress)
                else:
                    print("Could not find file specified.")

        case "lcd":
            if l < 2:
                print("USAGE: lcd <dir>")

            else:
                _path = sanitize_local_path(args[1], localpath)
                if _path:
                    localpath = _path
                else:
                    print("Local path does not exist.")

        case "cd":
            if l < 2:
                print("USAGE: cd <dir>")
            else:
                _path = sanitize_remote_path(path, args[1])
                if _path:
                    path = _path
                else:
                    print("Remote path does not exist.")

        case "lls":
            try:
                print("\n", localpath)
                for entry in listdir(localpath):
                    print("\t", entry,
                    "(FILE)" if ospath.isfile(localpath + entry)
                    else "(DIRECTORY)")
                print()
            except Exception as e:
                print("Could not read local path:")
                print(e)

        case "ls":
            try:
                print("\n", path)
                for entry in client.list(path, True):
                    print("\t", entry[0], f'({entry[1]["type"]})')
                print()
            except Exception as e:
                print("Could not read remote path:")
                print(e)

        case "delete":
            if l < 2:
                print("USAGE: delete <file>")
            else:
                try:
                    if not client.delete(path + str(args[1])):
                        print(args[1], "does not exist.")
                except Exception as e:
                    print("Could not delete", args[1] + ":")
                    print(e)

        case "mkdir":
            if l < 2:
                print("USAGE: mkdir <directory>")
            else:
                try:
                    args[1] = args[1].removesuffix("/")
                    if args[1].startswith("/"):
                        client.makedirs(args[1], 600)
                    else:
                        args[1] = args[1].removeprefix(".").removeprefix("/")
                        client.makedirs(path + "/" + args[1], 600)

                except Exception as e:
                    print("Could not create directory:")
                    print(e)

        # TODO: Add command descriptions
        case "help":
            print("Available commands:")
            print("mkdir")
            print("delete")
            print("put")
            print("get")
            print("ls")
            print("lls")
            print("cd")
            print("lcd")
            print("q/quit/exit")
            print("help")

        case "q" | "quit" | "exit":
            quit = True
            print("Quitting...")

        case _:
            print("Unknown command:", args[0])