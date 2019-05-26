import socket, os
from struct import pack, unpack_from
from hashlib import md5
from pg_lib.CONST import NULL_BYTE
from pg_lib import CONST

class MessageHandler():

    def __init__(self):
        self.code_map = {
                CONST.PARSE_COMPLETE: self.parse_complete,
                CONST.PARAMETER_DESCRIPTION: self.parameter_description,
                CONST.ROW_DESCRIPTION: self.row_description,
                CONST.BIND_COMPLETE: '',
                CONST.DATA_ROW: '',
                CONST.EMPTY_QUERY_RESPONSE: self.empty_query_response,
                CONST.READY_FOR_QUERY: self.ready_for_query
                }

    def handle(self, code, data):
        handle_func = self.default_message if self.code_map[code] is None else self.code_map[code]
        handle_func(data)

    def empty_query_response(self, data):
        raise Exception('query was empty')

    def parse_complete(self, data):
        pass

    def parameter_description(self, data):
        pass

    def ready_for_query(self, data):
        pass

    def row_description(self, data):
        row_count = unpack_from('!h', data)[0]
        idx = 2
        for i in range(row_count):
            pass
        print(data)

    def default_message(self, data):
        pass

class Connection():

    def __init__ (self,
            username='postgres',
            password='',
            host='localhoost',
            port=5432,
            db=None,
            connect_timeout=None):
        self.encoding = 'utf8'
        self.host = host
        self.username = username.encode(self.encoding) if isinstance(username, str) else username
        self.password = password.encode(self.encoding) if isinstance(password, str) else password
        self.db = db
        self.port = port
        self.connect_timeout = connect_timeout
        self._write_timeout = connect_timeout
        self._read_timeout = connect_timeout
        self._sock = None
        self._server_info = []
        self.handler = MessageHandler()
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
            self._login()
            self._get_pgserver_info()
            self._ready_for_query()
        except socket.error as e:
            self._sock.close()
            raise

    def execute(self, statement):
        pid = os.getpid()
        statement_name_bin = 'pg_statement_{}_1'.format(pid).encode('ascii') + NULL_BYTE
        msg  = statement_name_bin + statement.encode(self.encoding) + NULL_BYTE * 3
        self._send_message(CONST.PARSE, msg)
        self._send_message(CONST.DESCRIBE, CONST.STATEMENT + statement_name_bin)
        self._write_bytes(CONST.SYNC_MSG)
        self._rfile.flush()
        self.handle_messages()

    # https://www.postgresql.org/docs/9.1/protocol-message-formats.html
    def _startup_message(self):
        protocol = 196608
        msg = pack('!i', protocol) + b'user\x00' + self.username + NULL_BYTE
        if self.db is not None:
            database = self.db.encode('utf8') if isinstance(self.db, str) else self.db
            msg = msg + b'database\x00' + database + NULL_BYTE
        msg = msg + NULL_BYTE
        self._write_bytes(pack('!i', len(msg) + 4))
        self._write_bytes(msg)

    def _login(self):
        code, length = self._read_code_length()
        if code != CONST.AUTHENTICATION_REQUEST:
            return
        data = self._read_bytes(length - 4)
        auth_code = unpack_from('!i', data)[0]

        # only support md5
        if auth_code != 5:
            return
        if self.password is None:
            raise Exception('password is required for md5 auth')
        salt = b''.join(unpack_from('!cccc', data, 4))
        pwd = b'md5' + md5(
                md5(self.password + self.username).hexdigest().encode('ascii') +
                salt).hexdigest().encode('ascii')
        self._send_message(CONST.PASSWORD, pwd + NULL_BYTE)
        self._login()

    def _get_pgserver_info(self):
        code, length = self._read_code_length()
        if code != CONST.PARAMETER_STATUS:
            if code == CONST.BACKEND_KEY_DATA:
                self._get_backend_data(length)
            return
        data = self._read_bytes(length - 4)
        pos = data.find(NULL_BYTE)
        key, value = data[:pos], data[pos + 1:-1]
        self._server_info.append((key, value))
        self._get_pgserver_info()

    def _get_backend_data(self, length):
        self._backend_key_data = self._read_bytes(length - 4)

    def _ready_for_query(self):
        code, length = self._read_code_length()
        data = self._read_bytes(length - 4)
        if code != CONST.READY_FOR_QUERY:
            raise Exception('Not ready for query')

    def handle_messages(self):
        code = None
        while code != CONST.READY_FOR_QUERY:
            code, length = unpack_from('!ci', self._read_bytes(5))
            self.handler.handle(code, self._read_bytes(length - 4))


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

    def _read_code_length(self):
        return unpack_from('!ci', self._read_bytes(5))

    def _send_message(self, code, data):
        try:
            self._write_bytes(code)
            self._write_bytes(pack('!i', len(data) + 4))
            self._write_bytes(data)
            self._write_bytes(CONST.FLUSH_MSG)
        except:
            raise

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
    sql = 'select * from country limit 10;'
    conn.execute(sql)

if __name__ == '__main__':
    main()
