import hashlib
from io import BufferedReader

from ._crc32c import CRC32C
from .s3_etag import S3Etag


class ChecksummingBufferedReader:

    def __init__(self, file_handler, read_chunk_size, *args, **kwargs):
        """
        :param file_handler: the file handler to read from
        :param read_file_size: file size for correctly setting the s3 etag chunk size
        """
        self._hashers = dict(crc32c=CRC32C(),
                             sha1=hashlib.sha1(),
                             sha256=hashlib.sha256(),
                             s3_etag=S3Etag(read_chunk_size))
        self._reader = BufferedReader(file_handler, *args, **kwargs)
        self.raw = self._reader.raw

    def read(self, size=None):
        chunk = self._reader.read(size)
        if chunk:
            for hasher in self._hashers.values():
                hasher.update(chunk)
        return chunk

    def get_checksums(self):
        checksums = {}
        checksums.update({name: hasher.hexdigest() for name, hasher in self._hashers.items()})
        return checksums

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
