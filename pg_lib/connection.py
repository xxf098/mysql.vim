import socket
from pg_lib import CONST

class Connection():

    def __init__ (self,
            username='postgres',
            password='',
            host='localhoost',
            port=5432,
            db=None,
            connect_timeout=None):
        self.host = host
        self.username = username.encode('utf8') if isinstance(username, str) else username
        self.password = password.encode('utf8') if isinstance(password, str) else password
        self.db = db
        self.port = port
        self.connect_timeout = connect_timeout
        self.encoding = 'utf8'
        self.connect()

    def connect(self):
        try:
            self._usock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.connect_timeout is not None:
                self._usock.settimeout(self.connect_timeout)
            self._usock.connect((self.host, self.port))
            self._sock = self._usock.makefile(mode="rwb")
            self._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            self._startup_message()
        except socket.error as e:
            self._usock.close()
            raise

    # https://www.postgresql.org/docs/9.1/protocol-message-formats.html
    def _startup_message(self):
        protocol = 196608

def main():
    conn = Connection(
            username="pguser",
            host='localhost',
            port=5432,
            db="testdb",
            password="123456",
            connect_timeout=None)

if __name__ == '__main__':
    main()
