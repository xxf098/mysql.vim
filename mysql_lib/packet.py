import struct
from . import CONST

class Packet(object):
    def __init__(self, data):
        self._data = data
        self._position = 0

    def get_all(self):
        return self._data

    def read(self, size):
        result = self._data[self._position:(self._position+size)]
        if len(result) != size:
            error = ('Result length not requested length:\n'
                     'Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s'
                     % (size, len(result), self._position, len(self._data)))
            raise AssertionError(error)
        self._position += size
        return result

    def read_length_encoded_integer(self):
        c = self.read_uint8()
        if c == CONST.NULL_COLUMN:
            return None
        if c < CONST.UNSIGNED_CHAR_COLUMN:
            return c
        elif c == CONST.UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == CONST.UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == CONST.UNSIGNED_INT64_COLUMN:
            return self.read_uint64()

    def read_length_coded_string(self):
        length = self.read_length_encoded_integer()
        if length is None:
            return None
        return self.read(length)

    def read_uint8(self):
        result = self._data[self._position]
        self._position += 1
        return result

    def read_uint16(self):
        result = struct.unpack_from('<H', self._data, self._position)[0]
        self._position += 2
        return result

    def read_uint24(self):
        low, high = struct.unpack_from('<HB', self._data, self._position)
        self._position += 3
        return low + (high << 16)

    def read_uint32(self):
        result = struct.unpack_from('<I', self._data, self._position)[0]
        self._position += 4
        return result

    def read_uint64(self):
        result = struct.unpack_from('<Q', self._data, self._position)[0]
        self._position += 8
        return result

    def is_eof_packet(self):
        return self._data[0:1] == b'\xfe' and len(self._data) < 9

    def is_error_packet(self):
        return self._data[0:1] == b'\xff'

    def check_error(self):
        if self.is_error_packet():
            raise Exception(self._data.decode())

    def read_struct(self, fmt):
        s = struct.Struct(fmt)
        result = s.unpack_from(self._data, self._position)
        self._position += s.size
        return result

class ColumnPacket(Packet):
    def __init__(self, data, encoding='utf8'):
        Packet.__init__(self, data)
        self._parse_field_descriptor(encoding)

    def _parse_field_descriptor(self, encoding):
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(encoding)
        self.org_table = self.read_length_coded_string().decode(encoding)
        self.name = self.read_length_coded_string().decode(encoding)
        self.org_name = self.read_length_coded_string().decode(encoding)
        self.charsetnr, self.length, self.type_code, self.flags, self.scale = (
            self.read_struct('<xHIBHBxx'))

    def description(self):
        return (
            self.name,
            self.type_code,
            None,
            self.get_column_length(),
            self.get_column_length(),
            self.scale,
            self.flags % 2 == 0)

    def get_column_length(self):
        if self.type_code == CONST.VAR_STRING:
            mblen = CONST.MBLENGTH.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length
