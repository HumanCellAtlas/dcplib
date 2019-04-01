import logging
import os
import struct
import sys

logger = logging.getLogger(__name__)
PYTHON_VERSION_CUTOFF = '3.5'


class CRC32C:
    def __init__(self, data=None):
        try:
            from crc32c import crc32
        except ImportError as e:
            logger.warning("crc32c: %s. Switching to software mode, which may be slow." % e)
            os.environ["CRC32C_SW_MODE"] = "auto"
            from crc32c import crc32
        self._checksum_value = crc32(data if data is not None else b"")
        self._crc32c = crc32

    def update(self, data):
        self._checksum_value = self._crc32c(data, self._checksum_value)

    def hexdigest(self):
        _checksum_byte_value = self._long_to_bytes(self._checksum_value)
        if sys.version < PYTHON_VERSION_CUTOFF:
            import binascii
            return binascii.hexlify(_checksum_byte_value).decode("utf-8").zfill(8)
        else:
            return _checksum_byte_value.hex().zfill(8)

    def _long_to_bytes(self, long_value):
        """
        Turns a long value into its byte string equivalent.
        :param long_value: the long value to be returned as a byte string
        :return: a byte string equivalent of a long value
        """
        _byte_string = b''
        pack = struct.pack
        while long_value > 0:
            _byte_string = pack(b'>I', long_value & 0xffffffff) + _byte_string
            long_value = long_value >> 32
        for i in range(len(_byte_string)):
            if _byte_string[i] != b'\000'[0]:
                break
        else:
            _byte_string = b'\000'
            i = 0
        _byte_string = _byte_string[i:]

        return _byte_string
