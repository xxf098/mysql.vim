import hashlib
from functools import partial
sha1_new = partial(hashlib.new, 'sha1')

def encrypt_password(password, message):
    if not password:
        return b''

    stage1 = sha1_new(password).digest()
    stage2 = sha1_new(stage1).digest()
    s = sha1_new()
    s.update(message[:20])
    s.update(stage2)
    result = s.digest()
    return _get_bytes(result, stage1)

def _get_bytes(message1, message2):
    result = bytearray(message1)
    for i in range(len(result)):
        result[i] ^= message2[i]
    return bytes(result)
