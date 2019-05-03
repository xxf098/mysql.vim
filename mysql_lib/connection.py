import socket, os, json, traceback, struct
from mysql_lib import CAPABILITIES, CONNECT_WITH_DB, PLUGIN_AUTH, MAX_PACKET_LEN


def byte2int(b):
    if isinstance(b, int):
        return b
    else:
        return struct.unpack("!B", b)[0]


def int2byte(i):
    return struct.pack("!B", i)


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
        client_flag |= CAPABILITIES
        if self.db:
            client_flag |= CONNECT_WITH_DB
        self.client_flag = client_flag

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

class Connection:
    def __init__(self, config=None):
        self.host = config.host
        self.user = config.user
        self.password = config.password
        self.db = config.db
        self.port = config.port
        self.connect_timeout = config.connect_timeout
        self.encoding = config.encoding
        self._read_timeout = 120
        self._write_timeout = 120
        self.connect()

    def connect(self):
        try:
            address = (self.host, self.port)
            sock = socket.create_connection(address, self.connect_timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._sock = sock
            self._rfile = sock.makefile('rb')
            self._next_seq_id = 0

            self._get_server_information()
        except Exception as e:
            self._force_close()
            raise

    def _get_server_information(self):
        i = 0
        packet = self._read_packet()
        data = packet.get_all()

        self.protocol_version = byte2int(data[i:i+1])
        i += 1

        server_end = data.find(b'\0', i)
        self.server_version = data[i:server_end].decode('latin1')
        i = server_end + 1

        self.server_thread_id = struct.unpack('<I', data[i:i+4])
        i += 4

        self.salt = data[i:i+8]
        i += 9  # 8 + 1(filler)

        self.server_capabilities = struct.unpack('<H', data[i:i+2])[0]
        i += 2

        if len(data) >= i + 6:
            lang, stat, cap_h, salt_len = struct.unpack('<BHHB', data[i:i+6])
            i += 6
            self.server_language = lang
            self.server_status = stat
            self.server_capabilities |= cap_h << 16
            salt_len = max(12, salt_len - 9)

        i += 10

        if len(data) >= i + salt_len:
            self.salt += data[i:i+salt_len]
            i += salt_len

        i+=1
        if self.server_capabilities & PLUGIN_AUTH and len(data) >= i:
            server_end = data.find(b'\0', i)
            self._auth_plugin_name = (data[i:] if server_end < 0 else data[i:server_end]).decode('utf-8')

    def _read_packet(self):
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
            if bytes_to_read < MAX_PACKET_LEN:
                break
        packet = Packet(buff)
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

    def _force_close(self):
        if self._sock:
            try:
                self._sock.close()
            except:
                pass
        self._sock = None
        self._rfile = None

class Packet(object):
    def __init__(self, data):
        self._data = data

    def get_all(self):
        return self._data

    def is_error_packet(self):
        return self._data[0:1] == b'\xff'

    def check_error(self):
        if self.is_error_packet():
            raise Exception('fail to check packet')

def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.abspath(os.path.join(dir_path, os.pardir))
    config_path = os.path.join(parent_dir, 'config.json')
    config = DBConfig.load(config_path)
    connection = Connection(config)
    print('this is the end')

if __name__ == '__main__':
    main()
