from struct import pack
def create_message(code, data=b''):
    return code + pack('!i', len(data) + 4) + data

NULL_BYTE = b'\x00'
NOTICE_RESPONSE = b'N'
AUTHENTICATION_REQUEST = b'R'
PARAMETER_STATUS = b'S'
BACKEND_KEY_DATA = b'K'
READY_FOR_QUERY = b'Z'
ROW_DESCRIPTION = b'T'
ERROR_RESPONSE = b'E'
DATA_ROW = b'D'
COMMAND_COMPLETE = b'C'
PARSE_COMPLETE = b'1'
BIND_COMPLETE = b'2'
CLOSE_COMPLETE = b'3'
PORTAL_SUSPENDED = b's'
NO_DATA = b'n'
PARAMETER_DESCRIPTION = b't'
NOTIFICATION_RESPONSE = b'A'
COPY_DONE = b'c'
COPY_DATA = b'd'
COPY_IN_RESPONSE = b'G'
COPY_OUT_RESPONSE = b'H'
EMPTY_QUERY_RESPONSE = b'I'
BIND = b'B'
PARSE = b'P'
EXECUTE = b'E'
FLUSH = b'H'
SYNC = b'S'
PASSWORD = b'p'
DESCRIBE = b'D'
TERMINATE = b'X'
CLOSE = b'C'
STATEMENT = b'S'
PORTAL = b'P'
FLUSH = b'H'
FLUSH_MSG = create_message(FLUSH)
SYNC_MSG = create_message(SYNC)
