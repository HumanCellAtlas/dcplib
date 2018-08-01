#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals
import io, os, sys, unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dcplib import s3_multipart
from dcplib.checksumming_io import ChecksummingBufferedReader
from . import TEST_FILE, TEST_FILE_CHECKSUMS


class TestChecksummingBufferedReader(unittest.TestCase):

    file_size = os.path.getsize(TEST_FILE)
    chunk_size = s3_multipart.get_s3_multipart_chunk_size(file_size)

    def check_sums(self, checksums):
        self.assertEqual(checksums['sha1'], TEST_FILE_CHECKSUMS['sha1'])
        self.assertEqual(checksums['sha256'], TEST_FILE_CHECKSUMS['sha256'])
        self.assertEqual(checksums['crc32c'].lower(), TEST_FILE_CHECKSUMS['crc32c'].lower())
        self.assertEqual(checksums['s3_etag'], TEST_FILE_CHECKSUMS['s3_etag'])

    def test_checksums_after_single_read(self):
        with io.open(TEST_FILE, 'rb') as fh:
            reader = ChecksummingBufferedReader(fh, self.chunk_size)
            reader.read()
            sums = reader.get_checksums()
            self.check_sums(sums)

    def test_checksums_after_multiple_reads(self):
        with io.open(TEST_FILE, 'rb') as raw_fh:
            reader = ChecksummingBufferedReader(raw_fh, self.chunk_size)
            while True:
                buf = reader.read(self.chunk_size)
                if not buf:
                    break
            sums = reader.get_checksums()
            self.check_sums(sums)


if __name__ == '__main__':
    unittest.main()
