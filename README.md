# hdfs_client

Simple CLI for hadoop's HDFS.

## Usage:

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

- [x] Rewrite without dependencies.
- [ ] Finish implementing all methods as commands:
    - recursive for `delete`
    - `touch`
    - `exists`
    - `status`
- [ ] Optimize uploading/downloading.
