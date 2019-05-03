import socket, os, json, traceback
class DBConfig:
    def __init__(self, host='localhost', user='root', password='', db=None, port=3306,
            connect_timeout=None,charset=''):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.connect_timeout = connect_timeout

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
        self.config = config

    def connect(self):
        try:
            address = (self.config.host, self.config.port)
            sock = socket.create_connection(address, self.config.connect_timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._sock = sock
        except Exception as e:
            raise

    def _read_packet(self):
        pass
    def _read_bytes(self, num_bytes):
        pass

def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.abspath(os.path.join(dir_path, os.pardir))
    config_path = os.path.join(parent_dir, 'config.json')
    config = DBConfig.load(config_path)
    connection = Connection(config)
    print('this is the end')

if __name__ == '__main__':
    main()
