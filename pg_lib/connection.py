import socket, os
from struct import pack, unpack_from
from hashlib import md5
from pg_lib.CONST import NULL_BYTE
from pg_lib import CONST
from collections import defaultdict
import datetime
from calendar import timegm

class MessageHandler():

    def __init__(self, encoding='utf8'):
        self.encoding = encoding
        self.code_map = {
                CONST.PARSE_COMPLETE: self.parse_complete,
                CONST.PARAMETER_DESCRIPTION: self.parameter_description,
                CONST.ROW_DESCRIPTION: self.row_description,
                CONST.BIND_COMPLETE: self.bind_complete,
                CONST.DATA_ROW: self.data_row,
                CONST.EMPTY_QUERY_RESPONSE: self.empty_query_response,
                CONST.READY_FOR_QUERY: self.ready_for_query,
                CONST.COMMAND_COMPLETE: self.command_complete,
                CONST.ERROR_RESPONSE: self.error_response
                }

    def handle(self, code, data, response):
        handle_func = self.default_message if self.code_map[code] is None else self.code_map[code]
        handle_func(data, response)
        return response

    def empty_query_response(self, data, response):
        raise Exception('query was empty')

    def parse_complete(self, data, response):
        pass

    def parameter_description(self, data, response):
        pass

    def ready_for_query(self, data, response):
        pass

    def row_description(self, data, response):
        idx = 2
        result = []
        while idx < len(data):
            name = data[idx:data.find(NULL_BYTE, idx)]
            idx += len(name) + 1
            field = dict(
                zip((
                    "table_oid", "column_attrnum", "type_oid", "type_size",
                    "type_modifier", "format"), unpack_from('!ihihih', data, idx)))
            field['name'] = name
            idx += 18
            result.append(field)
        # 23 1043 1114
        response['row_desc'] = result

    def bind_complete(self, data, response):
        pass

    def data_row(self, data, response):
        idx = 2
        row = []
        for parse_func in response['parse_funcs']:
            length = unpack_from('!i', data, idx)[0]
            idx += 4
            if length == -1:
                row.append(None)
            else:
                row.append(parse_func(data, idx, length))
                idx += length
        response['rows'].append(row)

    def command_complete(self, data, response):
        values = data[:-1].split(b' ')
        response['row_count'] = int(values[-1])

    def default_message(self, data, response):
        pass

    def error_response(self, data, response):
        msg = dict((s[:1].decode(self.encoding), s[1:].decode(self.encoding))
                for s in data.split(NULL_BYTE) if s != b'')
        return msg


def parse_int4(data, offset, length):
    return unpack_from('!i', data, offset)[0]

EPOCH = datetime.datetime(2000, 1, 1)
EPOCH_SECONDS = timegm(EPOCH.timetuple())
def parse_timestamp(data, offset, length):
    return datetime.datetime.utcfromtimestamp(EPOCH_SECONDS + unpack_from('!d', data, offset)[0])

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
        self._init_type_info()
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
        response = self.handle_messages(response = { 'rows': [] })
        response['parse_funcs'] = tuple(self.type_info[f['type_oid']][1] for f in response['row_desc'])

        column_format = tuple(self.type_info[f['type_oid']][0] for f in response['row_desc'])
        bind_info = NULL_BYTE + statement_name_bin + NULL_BYTE * 4
        bind_info = bind_info + pack('!h', len(column_format)) + pack('!' + 'h'* len(column_format), *column_format)

        self._send_message(CONST.BIND, bind_info)
        self._write_bytes(CONST.EXECUTE_MSG)
        self._write_bytes(CONST.FLUSH_MSG)
        self._write_bytes(CONST.SYNC_MSG)
        self._rfile.flush()
        self.handle_messages(response)
        return { 'rows': response['rows'] }

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

    def handle_messages(self, response = {}):
        code = None
        while code != CONST.READY_FOR_QUERY:
            code, length = unpack_from('!ci', self._read_bytes(5))
            response =  self.handler.handle(code, self._read_bytes(length - 4), response)
        return response


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

    def _init_type_info(self):
        def parse_text(data, offset, length):
            return str(data[offset: offset + length], self.encoding)
        self.type_info = defaultdict(
                lambda: (CONST.TEXT_FORMAT, parse_text), {
                    23: (CONST.BINARY_FORMAT, parse_int4),
                    1043: (CONST.BINARY_FORMAT, parse_text),
                    1114: (CONST.BINARY_FORMAT, parse_timestamp)
                })

    def close(self):
        try:
            self._write_bytes(CONST.TERMINATE_MSG)
            self._rfile.flush()
            self._sock.close()
        except:
            pass
        finally:
            self._sock = None
            self._rwfile = None

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
    result = conn.execute(sql)
    conn.close()
    for row in result['rows']:
        print(row)

if __name__ == '__main__':
    main()
