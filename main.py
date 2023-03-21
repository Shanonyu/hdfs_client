#!/usr/bin/env python3
# encoding UTF-8

if __name__ == "__main__":

    from rest_hdfs import Client
    from util      import sanitize_local_path, sanitize_remote_path, split_with_quotes

    from sys  import argv
    from os   import listdir
    from os   import path as ospath
    from os   import name as osname

    if len(argv) != 4:
        print("Python HDFS command line interface")
        print(f"USAGE: {argv[0]} <address> <port> <user>")
        exit(1)

    adress = argv[1]
    if not adress.startswith("http"):
        adress = "http://" + adress
    port   = argv[2]
    user   = argv[3]

    print("Connecting:", adress + ":" + port, "as", user + "...")
    client = Client(adress, port, user)

    try:
        if not client.exists("/"): raise Exception("Root path does not exist")
    except Exception as e:
        print("Could not connect to HDFS:")
        print(e)
        print("Check if adress is correct.")
        exit(1)

    path = "/"
    localpath = ospath.expanduser("~") + "/"

    # Hecking windows
    if (osname == "nt"):
        localpath = localpath.replace('\\', '/')

    # TODO: Make use of path sanitizing functions for all commands
    while (True):

        print(f"[hdfs: {path}]")
        print(f"[user: {localpath}]")
        print(user + " # ", end="")
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
                                print("Could not append files:")
                                print(e)
                        else:
                            print("Could not find local file specified.")
                    else:
                        print("Could not find remote file specified.")

            case "put":
                if l != 2:
                    print("USAGE: put <file>")
                    print("Puts local file from current directory in current remote directory.")
                else:
                    try:
                        _path = localpath + args[1]
                        if ospath.lexists(_path):
                            with open(_path) as file:
                                client.upload(path, args[1], file)
                        else:
                            print("Could not find file specified.")
                    except Exception as e:
                        print("Could not upload file specified:")
                        print(e)
                        print("Make sure you configured dataNode IP")

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
                        print("Make sure you configured dataNode IP")

            case "lcd":
                if l != 2:
                    print("USAGE: lcd <dir>")
                else:
                    _path = sanitize_local_path(localpath, args[1])
                    if _path:
                        localpath = _path
                    else:
                        print("Local path does not exist.")

            case "cd":
                if l < 2:
                    print("USAGE: cd <dir>")
                else:
                    _path = sanitize_remote_path(path, args[1], client)
                    if _path:
                        path = _path
                    else:
                        print("Remote path does not exist.")

            case "lls":
                try:
                    print("\n", localpath)
                    for entry in listdir(localpath):
                        _type = "FILE\t" if ospath.isfile(localpath + entry) else "DIRECTORY"
                        print("\t", _type, "\t", entry)
                    print()
                except Exception as e:
                    print("Could not read local path:")
                    print(e)

            case "ls":
                files, empty, err = client.ls(path)
                if err:
                    print(err)
                    continue
                print("\n", path)
                if empty:
                    continue
                # This is weird
                for entry in files:
                    _type = entry["type"]
                    __type = "FILE\t" if _type == "FILE" else _type
                    print("\t", __type, "\t", entry["pathSuffix"])
                print()

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
                    args[1] = args[1].removesuffix("/")
                    _path = ""
                    if args[1].startswith("/"):
                        _path = args[1]
                    else:
                        args[1] = args[1].removeprefix(".").removeprefix("/")
                        _path = path + args[1] if path != "/" else "/" + args[1]
                    if not client.mkdir(_path):
                        print("Could not create remote directory.")

            # TODO: Add command descriptions
            case "help":
                print("Available commands:")
                print("\tmkdir")
                print("\tdelete")
                print("\tput")
                print("\tget")
                print("\tls")
                print("\tlls")
                print("\tcd")
                print("\tlcd")
                print("\texit")
                print("\thelp")

            case "q" | "quit" | "exit":
                print("Quitting...")
                exit(0)

            case _:
                print("Unknown command:", args[0])
