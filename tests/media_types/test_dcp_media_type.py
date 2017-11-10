import os
import unittest

from dcplib.media_types import DcpMediaType


class TestDcpMediaType(unittest.TestCase):

    BUNDLE_FIXTURE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures'))

    TEST_FILES_AND_STRINGS = {
        'assay.json': 'application/json; dcp-type="metadata/assay"',
        'project.json': 'application/json; dcp-type="metadata/project"',
        'sample.json': 'application/json; dcp-type="metadata/sample"',
        'manifest.json': 'application/json; dcp-type=metadata',
        'SRR2963332_1.fastq.gz': 'application/gzip; dcp-type=data',
        'demo.gene.counts.txt': 'text/plain; dcp-type=data',
        'mystery_file': 'application/octet-stream; dcp-type=data'
    }

    def test_file_recognition(self):
        for filename, media_type in TestDcpMediaType.TEST_FILES_AND_STRINGS.items():
            file_path = os.path.join(self.BUNDLE_FIXTURE_DIR, filename)
            self.assertEqual(str(DcpMediaType.from_file(file_path)), media_type)
