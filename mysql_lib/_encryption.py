from hashlib import sha1
import struct

def encrypt_password(password, message):
    if not password:
        return b''

    hash1 = sha1(password).digest()
    hash2 = sha1(hash1).digest()
    hash3 = sha1(message + hash2).digest()
    xored = [h1 ^ h3 for (h1, h3) in zip(hash1, hash3)]
    hash4 = struct.pack('20B', *xored)
    return hash4

def _get_bytes(message1, message2):
    result = bytearray(message1)
    for i in range(len(result)):
        result[i] ^= message2[i]
    return bytes(result)
