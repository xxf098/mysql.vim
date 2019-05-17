import socket

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

