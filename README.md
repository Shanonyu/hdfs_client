# hdfs_client

Simple CLI for hadoop's HDFS ~~that uses python's [`hdfs`](https://hdfscli.readthedocs.io/en/latest/) library~~.

This branch is supposed to be working without dependencies.

## Usage:

Launching client:
```console
./main.py <HDFS host adress> <HDFS port> <username>
```

## Features:

- `mkdir`
    Creates a directory in HDFS.
- `delete`
    Deletes a file in HDFS.
- `put`
    Uploads a file/directory to HDFS.
- `get`
    Downloads a file/directory from HDFS.
- `ls`
    Lists current HDFS directory.
- `lls`
    Lists current local directory.
- `cd`
    Changes current HDFS directory.
- `lcd`
    Changes current local directory.
- `exit`
    Exit from CLI.
- `help`
    Lists all commands.

## TODO

- Rewrite without dependencies.
- ~~File transfers on Windows cause http errors. (Windows users should consider uninstalling)~~ File transfers cause errors when you don't have proper node's IP adress configured.
