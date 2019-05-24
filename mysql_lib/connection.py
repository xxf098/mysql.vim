import socket, os, json, traceback, struct
from mysql_lib import CONST, _encryption, utils, Cursor
from mysql_lib.packet import Packet, ColumnPacket, OKPacket

class DBConfig:

    def __init__(self, host='localhost', user='root', password='', db=None, port=3306,
            connect_timeout=None, charset=''):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.connect_timeout = connect_timeout
        self.charset = 'utf8mb4'
        self.encoding = 'utf8'
        self.charset_id = 224
        client_flag = 0
        client_flag |= CONST.CAPABILITIES
        if self.db:
            client_flag |= CONST.CONNECT_WITH_DB
        self.client_flag = client_flag

    def  __getitem__(self, name):
        return super().__getattribute__(name)

    @staticmethod
    def load(path):
        try:
            with open(path) as config_file:
                data = json.load(config_file)
                if isinstance(data, list):
                    enableData = [x for x in data if x.get('enable', None) == True]
                    data = enableData[0] if len(enableData) > 0 else data[0]
                    data.pop('enable', None)
                return DBConfig(**data)
        except Exception as e:
            print(traceback.print_exc())
            exit(1)

class QueryResult(object):
    def __init__(self, connection):
        self.connection = connection
        self.affected_rows = None
        self.description = None
        self.rows = None
        self.field_count = 0

    # https://dev.mysql.com/doc/internals/en/com-query-response.html
    def read(self):
        try:
            first_packet = self.connection._read_packet()
            if first_packet._is_ok_packet():
                self._read_ok_packet(first_packet)
            else:
                self._read_result_packet(first_packet)
        finally:
            self.connection = None
    
    def _read_ok_packet(self, first_packet):
        ok_packet = OKPacket(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.message = ok_packet.message
        self.has_next = ok_packet.has_next
        self.rows = [[self.message.decode()]]     

    def _read_result_packet(self, first_packet):
        self.field_count = first_packet.read_length_encoded_integer()
        self._read_column_definition_packet()
        self._read_resultrow_packet()
        # print(self.field_count)

    def _read_column_definition_packet(self):
        self.columns = []
        column_names = []
        self.converters = []
        use_unicode = self.connection.use_unicode
        encoding = self.connection.encoding
        description = []
        for i in range(self.field_count):
            field = self.connection._read_packet(ColumnPacket)
            self.columns.append(field)
            column_names.append(field.name)
            description.append(field.description())
            field_type = field.type_code
            self.converters.append((encoding, None))

        eof_packet = self.connection._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)
        self.column_names = tuple(column_names)

    def _read_resultrow_packet(self):
        rows = []
        while True:
            packet = self.connection._read_packet()
            if packet.is_eof_packet():
                self.connection = None  # release reference to kill cyclic reference.
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        row = []
        for encoding, converter in self.converters:
            try:
                data = packet.read_length_coded_string()
            except IndexError:
                break
            if data is not None:
                if encoding is not None:
                    data = data.decode(encoding)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

class Connection():

    def __init__(self, config=None):
        self.encoding = config.encoding
        self.host = config.host
        self.user = utils.encode_str(config.user, self.encoding)
        self.password = utils.encode_str(config.password, 'latin1')
        self.db = config.db
        self.port = config.port
        self.connect_timeout = config.connect_timeout
        self._read_timeout = 120
        self._write_timeout = 120
        self.client_flag = config.client_flag
        self.charset_id = config.charset_id
        self.use_unicode = True
        self._sock = None
        self.connect()

    def connect(self):
        self._closed = False
        try:
            address = (self.host, self.port)
            sock = socket.create_connection(address, self.connect_timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._sock = sock
            self._rfile = sock.makefile('rb')
            self._next_seq_id = 0

            self._init_handshake()
            self._login()
        except Exception as e:
            self._force_close()
            raise

    def run_sql(self, sql):
        sql = sql.encode(self.encoding, 'surrogateescape')
        self._execute_query(CONST.COM_QUERY, sql)
        self._affected_rows = self._read_query_result()
        return self._result

    def cursor(self):
        return Cursor(self)

    def _execute_query(self, command, sql):
        if not self._sock:
            raise Exception('sock not found')

        if isinstance(sql, str):
            sql = sql.encode(self.encoding)

        packet_size = min(CONST.MAX_PACKET_LEN, len(sql) + 1)
        prelude = struct.pack('<iB', packet_size, command)
        packet = prelude + sql[:packet_size-1]
        self._write_bytes(packet)
        self._next_seq_id = 1

        #TODO: big query handle
        if packet_size < CONST.MAX_PACKET_LEN:
            return

    def _read_query_result(self):
        result = QueryResult(self)
        result.read()
        self._result = result
        return result.affected_rows

    def close(self):
        if self._closed:
            raise Exception('Already closed')
        self._closed = True
        if self._sock is None:
            return
        send_data = struct.pack('<iB', 1, CONST.COM_QUIT)
        try:
            self._write_bytes(send_data)
        except Exception:
            pass
        finally:
            self._force_close()

    # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
    def _init_handshake(self):
        i = 0
        packet = self._read_packet()
        data = packet.get_all()

        self.protocol_version = utils.byte2int(data[i:i+1])
        i += 1

        self.server_version, server_end = utils.read_str(data, end=b'\0', decode='latin1')
        i = server_end + 1

        self.connection_id,\
        self.salt,\
        self.server_capabilities,\
        self.server_language,\
        self.server_status,\
        cap_h,\
        auth_len\
        = struct.unpack('<I8sxHBHHB', data[i:i+21])
        self.server_capabilities |= cap_h << 16
        auth_len = max(12, auth_len - 9)
        i += 31

        if len(data) >= i + auth_len:
            self.salt += data[i:i+auth_len]
            i += auth_len

        i+=1
        if self.server_capabilities & CONST.PLUGIN_AUTH and len(data) >= i:
            self._auth_plugin_name, _ = utils.read_str(data[i:], end=b'\0', decode='utf-8')

    # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41
    def _login(self):
        self.client_flag |= CONST.MULTI_RESULTS
        charset_id = self.charset_id
        data = struct.pack('<iIB23s{}sx'.format(len(self.user)), self.client_flag, CONST.MAX_PACKET_LEN, charset_id, b'', self.user)

        data += self._get_encrypt_pass()

        if self.db and self.server_capabilities & CONST.CONNECT_WITH_DB:
            self.db = self.db.encode(self.encoding)
            data += self.db + b'\0'

        if self.server_capabilities & CONST.PLUGIN_AUTH:
            data += (self._auth_plugin_name.encode() or b'') + b'\0'

        connect_attrs = b''
        data += struct.pack('B', len(connect_attrs)) + connect_attrs

        self.write_packet(data)
        auth_packet = self._read_packet()

    def _get_encrypt_pass(self):
        encrypted_pass = b''
        data = b''
        if self._auth_plugin_name == '':
            encrypted_pass = _encryption.encrypt_password(self.password, self.salt)
        elif self._auth_plugin_name == 'mysql_native_password':
            plugin_name = b'mysql_native_password'
            encrypted_pass = _encryption.encrypt_password(self.password, self.salt)

        if self.server_capabilities & CONST.PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += utils.lenenc_int(len(encrypted_pass)) + encrypted_pass
        elif self.server_capabilities & CONST.SECURE_CONNECTION:
            data += struct.pack('B', len(encrypted_pass)) + encrypted_pass
        else:
            data += encrypted_pass + b'\0'
        return data

    def write_packet(self, payload):
        data = utils.pack_int24(len(payload)) + utils.int2byte(self._next_seq_id) + payload
        self._write_bytes(data)
        self._next_seq_id = (self._next_seq_id + 1) % 256

    def _read_packet(self, packet_class=Packet):
        buff = b''
        while True:
            packet_header = self._read_bytes(4)
            btrl, btrh, packet_number = struct.unpack('<HBB', packet_header)
            bytes_to_read = btrl + (btrh << 16)
            if packet_number != self._next_seq_id:
                self._force_close()
                raise Exception('fail to read packet')
            self._next_seq_id = (self._next_seq_id + 1) % 256
            recv_data = self._read_bytes(bytes_to_read)
            buff += recv_data
            if bytes_to_read == 0xffffff:
                continue
            if bytes_to_read < CONST.MAX_PACKET_LEN:
                break
        packet = packet_class(buff)
        packet.check_error()
        return packet

    def _read_bytes(self, num_bytes):
        self._sock.settimeout(self._read_timeout)
        try:
            data = self._rfile.read(num_bytes)
        except Exception as e:
            self._force_close()
            raise
        if len(data) < num_bytes:
            self._force_close()
            raise Exception('fail to read data')
        return data

    def _write_bytes(self, data):
        self._sock.settimeout(self._write_timeout)
        try:
            self._sock.sendall(data)
        except IOError as e:
            self._force_close()
            raise Exception('fail to write data')

    def _force_close(self):
        if not self._sock:
            return
        try:
            self._sock.close()
        except:
            pass
        self._sock = None
        self._rfile = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.abspath(os.path.join(dir_path, os.pardir))
    config_path = os.path.join(parent_dir, 'config.json')
    config = DBConfig.load(config_path)
    try:
        connection = Connection(config)
        # sql = "SELECT table_name FROM information_schema.tables WHERE table_type = 'base table' AND table_schema='{}'".format(config.db)
        # sql = 'show create table users;'
        sql = "UPDATE users SET name = '123' where id = '123456';"
        result = connection.run_sql(sql)
        [print(x) for row in result.rows for x in row]
    except Exception as e:
        print(traceback.print_exc())
    finally:
        if connection is not None:
            connection.close()

#TODO: python & vim ariac2 tool TBB
if __name__ == '__main__':
    main()
