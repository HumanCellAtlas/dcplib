import hashlib

"""
A digest generator that generates S3 ETAGs
with the same interface as the hashlib generators.
"""


class S3Etag:

    def __init__(self, chunk_size):
        self._etag_bytes = 0
        self._etag_parts = []
        self._etag_hasher = hashlib.md5()
        self._chunk_size = chunk_size

    @property
    def chunk_size(self):
        return self._chunk_size

    def update(self, chunk):
        if self._etag_bytes + len(chunk) > self.chunk_size:
            chunk_head = chunk[:self.chunk_size - self._etag_bytes]
            chunk_tail = chunk[self.chunk_size - self._etag_bytes:]
            self._etag_hasher.update(chunk_head)
            self._etag_parts.append(self._etag_hasher.digest())
            self._etag_hasher = hashlib.md5()
            self._etag_hasher.update(chunk_tail)
            self._etag_bytes = len(chunk_tail)
        else:
            self._etag_hasher.update(chunk)
            self._etag_bytes += len(chunk)

    def hexdigest(self):
        if self._etag_bytes:
            self._etag_parts.append(self._etag_hasher.digest())
            self._etag_bytes = 0
        if len(self._etag_parts) > 1:
            etag_csum = hashlib.md5(b"".join(self._etag_parts)).hexdigest()
            return '{}-{}'.format(etag_csum, len(self._etag_parts))
        else:
            return self._etag_hasher.hexdigest()
