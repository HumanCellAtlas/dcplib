import os
import unittest

from dcplib.media_types import MediaType


class TestMediaType(unittest.TestCase):

    TEST_STRINGS_AND_ATTRS = {
        'application/json': {
            'top_level_type': 'application', 'subtype': 'json'
        },
        'application/json+zip': {
            'top_level_type': 'application', 'subtype': 'json', 'suffix': 'zip',
        },
        'application/octet-stream+zip; dcp-type=data': {
            'top_level_type': 'application', 'subtype': 'octet-stream',
            'suffix': 'zip', 'parameters': {'dcp-type': 'data'}
        },
        'application/json; dcp-type="metadata/sample"': {
            'top_level_type': 'application', 'subtype': 'json', 'parameters': {'dcp-type': 'metadata/sample'}
        },
        'application/json; dcp-type="metadata/sample"; other-param=foo; quoted-param="bar/baz"': {
            'top_level_type': 'application', 'subtype': 'json',
            'parameters': {'dcp-type': 'metadata/sample', 'other-param': 'foo', 'quoted-param': 'bar/baz'}
        },
        'application/octet-stream; param-with-semicolon-in-value="!#%&~<>@;$*+-"; param2=value2': {
            'top_level_type': 'application', 'subtype': 'octet-stream',
            'parameters': {'param-with-semicolon-in-value': '!#%&~<>@;$*+-', 'param2': 'value2'}
        },
        'application/octet-stream; param-with-escaped-quote-in-value="foo\\"bar"; param2=value2': {
            'top_level_type': 'application', 'subtype': 'octet-stream',
            'parameters': {'param-with-escaped-quote-in-value': 'foo"bar', 'param2': 'value2'}
        }
    }

    def test_string_generation_from_attrs(self):
        for media_type, attributes in self.TEST_STRINGS_AND_ATTRS.items():
            mt = MediaType(**attributes)
            self.assertEqual(media_type, str(mt))

    def test_string_parsing_to_attrs_to_string_generation_round_trip(self):
        for media_type_string, attributes in self.TEST_STRINGS_AND_ATTRS.items():

            mt = MediaType.from_string(media_type_string)

            self.assertEqual(attributes['top_level_type'], mt.top_level_type)
            self.assertEqual(attributes['subtype'], mt.subtype)
            if 'suffix' in attributes:
                self.assertEqual(attributes['suffix'], mt.suffix)
            if 'parameters' in attributes:
                self.assertEqual(attributes['parameters'], mt.parameters)

            self.assertEqual(str(mt), media_type_string)

    BUNDLE_FIXTURE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures'))

    TEST_FILES_AND_STRINGS = {
        'assay.json': 'application/json',
        'SRR2963332_1.fastq.gz': 'application/gzip',
        'demo.gene.counts.txt': 'text/plain',
        'mystery_file': 'application/octet-stream'
    }

    def test_file_recognition(self):
        for filename, media_type in TestMediaType.TEST_FILES_AND_STRINGS.items():
            file_path = os.path.join(self.BUNDLE_FIXTURE_DIR, filename)
            self.assertEqual(str(MediaType.from_file(file_path)), media_type)
