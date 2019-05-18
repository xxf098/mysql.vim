import socket, struct
from pg_lib.CONST import NULL_BYTE

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
        self._write_timeout = connect_timeout
        self._read_timeout = connect_timeout
        self.encoding = 'utf8'
        self._sock = None
        self.connect()

    def connect(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.connect_timeout is not None:
                self._sock.settimeout(self.connect_timeout)
            self._sock.connect((self.host, self.port))
            self._rfile = self._sock.makefile(mode='rb')
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            self._startup_message()
        except socket.error as e:
            self._sock.close()
            raise

    # https://www.postgresql.org/docs/9.1/protocol-message-formats.html
    def _startup_message(self):
        protocol = 196608
        msg = struct.pack('!i', protocol) + b'user\x00' + self.username + NULL_BYTE
        if self.db is not None:
            database = self.db.encode('utf8') if isinstance(self.db, str) else self.db
            msg = msg + b'database\x00' + database + NULL_BYTE
        msg = msg + NULL_BYTE
        self._write_bytes(struct.pack('!i', len(msg) + 4))
        self._write_bytes(msg)
        code, length = struct.unpack('!ci', self._read_bytes(5))
        print(code)

    def _write_bytes(self, bytes):
        self._sock.settimeout(self._write_timeout)
        try:
            self._sock.sendall(bytes)
        except IOError as e:
            self._force_close()
            raise

    #TODO: refactor with mysql_lib
    def _read_bytes(self, num_bytes):
        self._sock.settimeout(self._read_timeout)
        try:
            data = self._rfile.read(num_bytes)
        except:
            self._force_close()
            raise
        if len(data) < num_bytes:
            self._force_close()
            raise Exception('fail to read data')
        return data

    def _force_close(self):
        if self._sock is None:
            return
        try:
            self._sock.close()
        except:
            pass
        self._sock = None
        self._rwfile = None


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
