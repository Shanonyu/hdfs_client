#!/usr/bin/env python3
# encoding UTF-8

if __name__ != "__main__": exit(0)

from rest_hdfs import Client
from util      import sanitize_local_path, sanitize_remote_path, split_with_quotes

from sys  import argv
from os   import listdir
from os   import path as ospath
from os   import name as osname

if len(argv) < 4:
    print(f"USAGE: {argv[0]} <address> <port> <user>")
    exit(0)

adress = argv[1]
if not adress.startswith("http"):
    adress = "http://" + adress
port   = argv[2]
user   = argv[3]

print("Connecting:", adress + ":" + port, "as", user + "...")
client = Client(adress, port, user)

try:
    client.exists("/")
except Exception as e:
    print("Could not connect to HDFS:")
    print(e)
    print("Check if address and port are valid.")
    exit(1)

quit = False
path = "/"
localpath = ospath.expanduser("~") + "/"

# Hecking windows (works)
if (osname == "nt"):
    localpath = localpath.replace('\\', '/')

while (not quit):

    print(f"[loc: {localpath}]")
    print(f"[rem: {path}]")
    print(user, "# ", end="")
    _cmd = input()
    args = split_with_quotes(_cmd)
    l = len(args)

    if not args: continue

    match args[0]:
        case "append":
            if l != 3:
                print("USAGE: append <local file> <remote file>")
                print("Appends contents from local file to remote file.")
            else:
                _localpath = localpath + args[1]
                _path = path + args[2]
                if client.exists(_path):
                    if ospath.lexists(_localpath):
                        # TODO: Read chunks at a time
                        # NOTE: This doesn't support big files
                        try:
                            with open(_localpath) as local_file:
                                    local_file_contents = local_file.read()
                                    res, err = client.append(path, args[2], local_file_contents)
                                    if err:
                                        print(err)

                        except Exception as e:
                            print("Could not read files:")
                            print(e)
                    else:
                        print("Could not find local file specified.")
                else:
                    print("Could not find remote file specified.")

        # NOTE: GET & PUT do not work if you didn't configure dataNode IP
        case "put":
            if l != 2:
                print("USAGE: put <file>")
                print("Puts local file from current directory in current remote directory.")
            else:
                try:
                    if ospath.lexists(localpath + args[1]):
                        file = client.upload(path, localpath + args[1])
                    else:
                        print("Could not find file specified.")
                except Exception as e:
                    print("Could not upload file specified:")
                    print(e)

        case "get":
            if l != 2:
                print("USAGE: get <file>")
                print("Puts remote file in current directory in current local directory.")
            else:
                try:
                    _path = path + args[1]
                    if client.exists(_path):
                        file = client.download(path, args[1], localpath)
                    else:
                        print("Could not find file specified.")
                except Exception as e:
                    print("Could not download file specified:")
                    print(e)
                    print("Make sure you configured TEMPORARY_REDIRECT endpoint")

        case "lcd":
            if l != 2:
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
                _path = sanitize_remote_path(client, path, args[1])
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
            files, err = client.ls(path)
            if err:
                print(err)
                continue
            for entry in files:
                print("\t", entry["pathSuffix"], f'({entry["type"]})')

        case "delete":
            if l < 2:
                print("USAGE: delete <file>")
            else:
                if not client.delete(path, args[1]):
                    print(args[1], "does not exist.")

        case "mkdir":
            if l < 2:
                print("USAGE: mkdir <directory>")
            else:
                try:
                    args[1] = args[1].removesuffix("/")
                    if args[1].startswith("/"):
                        client.mkdir(args[1])
                    else:
                        args[1] = args[1].removeprefix(".").removeprefix("/")
                        client.mkdir(path + args[1] if path != "/" else "/" + args[1])

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
            print("exit")
            print("help")

        case "q" | "quit" | "exit":
            quit = True
            print("Quitting...")

        case _:
            print("Unknown command:", args[0])
