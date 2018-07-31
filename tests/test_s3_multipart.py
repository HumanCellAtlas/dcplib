import unittest

from dcplib.s3_multipart import get_s3_multipart_chunk_size

MB = 1024 * 1024


class TestS3Multipart(unittest.TestCase):

    def test_get_s3_multipart_chunk_size(self):

        self.assertEqual(get_s3_multipart_chunk_size(1), 64 * MB)
        self.assertEqual(get_s3_multipart_chunk_size(10000 * 64 * MB), 64 * MB)
        self.assertEqual(get_s3_multipart_chunk_size(10000 * 64 * MB + 1), 65 * MB)
        self.assertEqual(get_s3_multipart_chunk_size(10000 * 65 * MB), 65 * MB)
        self.assertEqual(get_s3_multipart_chunk_size(10000 * 65 * MB + 1), 66 * MB)
