import hashlib
import crcmod
from .s3_etag import S3Etag

"""
A file-like object that computes checksums for the data written to it, discarding the actual data.
"""


class ChecksummingSink:

    def __init__(self, hash_functions=('crc32c', 'sha1', 'sha256', 's3_etag')):
        self._hashers = dict()
        for hasher in hash_functions:
            if hasher == 'crc32c':
                self._hashers['crc32c'] = crcmod.predefined.Crc("crc-32c")
            elif hasher == 'sha1':
                self._hashers['sha1'] = hashlib.sha1()
            elif hasher == 'sha256':
                self._hashers['sha256'] = hashlib.sha256()
            elif hasher == 's3_etag':
                self._hashers['s3_etag'] = S3Etag()

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
