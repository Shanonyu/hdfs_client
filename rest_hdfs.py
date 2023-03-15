
import requests as rq
from json import loads

class InsecureClient:
    _adress: str
    _user:   str

    def __init__(self, adress: str, port: str, user: str):
        self._adress = f"{adress}:{port}/webhdfs/v1/"
        self._user   = user

    def _get(self, path, op) -> dict:
        content = b""
        with rq.get(f"{self._adress}{path}?user.name={self._user}&op={op}") as res:
            content = res.content
        return loads(content)
    
    def _delete(self, path: str, op: str, recursive: bool) -> dict:
        content = b""
        with rq.delete(f"{self._adress}{path}?user.name={self._user}&op={op}" + ("&recursive=true" if recursive else "")) as res:
            content = res.content
        return loads(content)

    def ls(self, path: str) -> dict:
        return self._get(path, "LISTSTATUS")
    
    def status(self, path: str) -> dict:
        return self._get(path, "GETFILESTATUS")
    
    def delete(self, path: str, recursive: bool = False):
        return self._delete(path, "DELETE", recursive)
    
    @staticmethod
    def test():
        client = InsecureClient("http://94.198.128.80", "6969", "protoje")
        print("==================LS==================")
        print(client.ls("/"))
        print("================STATUS================")
        print(client.status("/user/protoje"))
        print("================DELETE================")
        print(client.delete("/user/protoje/test.txt"))

InsecureClient.test()


# http://<HOST>:<HTTP_PORT>/webhdfs/v1/<PATH>?user.name=<USER>&op=

# TODO:
# upload    : put
# download  : get
# content   : ls
# makedirs  : mkdir
# append    : append
