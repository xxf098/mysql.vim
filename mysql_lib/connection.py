import socket, os, json, traceback
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
        packet = self._read_packet()

    def _read_packet(self):
        packet_header = self._read_bytes(4)
        return packet_header

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


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.abspath(os.path.join(dir_path, os.pardir))
    config_path = os.path.join(parent_dir, 'config.json')
    config = DBConfig.load(config_path)
    connection = Connection(config)
    print('this is the end')

if __name__ == '__main__':
    main()
