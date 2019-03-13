import hashlib
import logging
import os
import struct
import sys

from .s3_etag import S3Etag

logger = logging.getLogger(__name__)
PYTHON_VERSION_CUTOFF = '3.5'

"""
A file-like object that computes checksums for the data written to it, discarding the actual data. Current accepted
checksum functions are crc32c, s3_etag, sha1, and sha256.
"""


class ChecksummingSink:
    class CRC32C:
        def __init__(self, data=None):
            try:
                import crc32c
            except ImportError as e:
                logger.warning("crc32c: %s. Switching to software mode, which may be slow." % e)
                os.environ["CRC32C_SW_MODE"] = "auto"
                import crc32c
            self._checksum_value = crc32c.crc32(data if data is not None else b"")
            self._crc32c = crc32c

        def update(self, data):
            self._checksum_value = self._crc32c.crc32(data, self._checksum_value)

        def hexdigest(self):
            _checksum_byte_value = self._long_to_bytes(self._checksum_value)
            if sys.version < PYTHON_VERSION_CUTOFF:
                import binascii
                return binascii.hexlify(_checksum_byte_value).decode("utf-8")
            else:
                return _checksum_byte_value.hex()

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

    def __init__(self, write_chunk_size, hash_functions=('crc32c', 'sha1', 'sha256', 's3_etag')):
        """ ChecksummingSink is initialized with a map from the name of the hash function to an object that
        represents the calculation function."""
        self._hashers = dict()
        for hasher in hash_functions:
            if hasher == 'crc32c':
                self._hashers['crc32c'] = self.CRC32C()
            elif hasher == 'sha1':
                self._hashers['sha1'] = hashlib.sha1()
            elif hasher == 'sha256':
                self._hashers['sha256'] = hashlib.sha256()
            elif hasher == 's3_etag':
                self._hashers['s3_etag'] = S3Etag(write_chunk_size)

    def write(self, data):
        for hasher in self._hashers.values():
            hasher.update(data)

    def get_checksums(self):
        checksums = {}
        checksums.update({name: hasher.hexdigest() for name, hasher in self._hashers.items()})
        return checksums

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
