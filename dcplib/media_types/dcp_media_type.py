import os

from .media_type import MediaType


class DcpMediaType(MediaType):

    DCP_METADATA_JSON_FILES = ('assay', 'project', 'sample')

    @classmethod
    def from_file(cls, file_path, dcp_type=None):
        media_type = super(DcpMediaType, cls).from_file(file_path)
        if dcp_type:
            media_type.parameters['dcp-type'] = dcp_type
        else:
            media_type.parameters['dcp-type'] = cls._dcp_type_param(media_type, os.path.basename(file_path))
        return media_type

    @staticmethod
    def _dcp_type_param(media_type, filename):
        if media_type.main_type == 'application/json':
            (filename_without_extension, ext) = os.path.splitext(filename)
            if filename_without_extension in DcpMediaType.DCP_METADATA_JSON_FILES:
                return "metadata/{filename}".format(filename=filename_without_extension)
            else:
                return "metadata"
        else:
            return "data"
