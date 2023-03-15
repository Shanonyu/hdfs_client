# hdfs_client

Simple CLI for hadoop's HDFS that uses python's [`hdfs`](https://hdfscli.readthedocs.io/en/latest/) library.

## Usage:

Install dependencies:
```console
pip install 'hdfs==2.7.0'
```
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
