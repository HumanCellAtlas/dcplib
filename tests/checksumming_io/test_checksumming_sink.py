#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dcplib import s3_multipart
from dcplib.checksumming_io import ChecksummingSink
from dcplib.checksumming_io._crc32c import CRC32C
from . import TEST_FILE, TEST_FILE_CHECKSUMS


class TestChecksummingSink(unittest.TestCase):
    file_size = os.path.getsize(TEST_FILE)
    chunk_size = s3_multipart.get_s3_multipart_chunk_size(file_size)

    def check_sums(self, checksums):
        [self.assertEqual(checksums[hash_function].lower(), TEST_FILE_CHECKSUMS[hash_function].lower()) for
         hash_function in checksums.keys()]

    def test_crc32c_calculation(self):
        crc32 = CRC32C()

        with open(TEST_FILE, 'rb') as fh:
            data = fh.read()
            crc32.update(data)
            checksum = crc32.hexdigest()

        self.assertEqual(checksum.lower(), TEST_FILE_CHECKSUMS['crc32c'].lower())

    def test_crc32_calculation_empty_data_is_zero_padded(self):
        crc32 = CRC32C()

        crc32.update(b"")
        checksum = crc32.hexdigest()

        self.assertEquals(checksum.lower(), "00000000")

    def test_checksums_after_single_write(self):
        sink = ChecksummingSink(self.chunk_size)
        with open(TEST_FILE, 'rb') as fh:
            data = fh.read()
            sink.write(data)
        sums = sink.get_checksums()
        self.check_sums(sums)

    def test_checksums_after_multiple_write(self):
        sink = ChecksummingSink(self.chunk_size)
        with open(TEST_FILE, 'rb') as fh:
            while True:
                data = fh.read(self.chunk_size)
                if not data:
                    break
                sink.write(data)
        sums = sink.get_checksums()
        self.check_sums(sums)

    def test_hash_function_list_is_configurable(self):
        checksums_to_compute = ['sha1', 's3_etag']
        sink = ChecksummingSink(self.chunk_size, hash_functions=checksums_to_compute)
        with open(TEST_FILE, 'rb') as fh:
            data = fh.read()
            sink.write(data)
        sums = sink.get_checksums()
        self.assertEqual(list(sorted(sums.keys())), sorted(checksums_to_compute))
        [self.assertEqual(TEST_FILE_CHECKSUMS[checksum].lower(), sums[checksum].lower()) for checksum in
         checksums_to_compute]


if __name__ == '__main__':
    unittest.main()
