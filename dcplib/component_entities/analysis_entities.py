from termcolor import colored

from dcplib.component_entities import EntityBase


class Workflow(EntityBase):
    """Model an analysis Workflow entity.

    A typical analysis Workflow response from the Secondary-analysis service (Cromwell API), will look like the
    following:
    {
        "end": "2019-01-06T20:35:19.533Z",
        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "labels": {
            "bundle-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "bundle-version": "2018-11-02T114842.872218Z",
            "caas-collection-name": "xxxx-prod",
            "cromwell-workflow-id": "cromwell-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "project_shortname": "project name",
            "project_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "workflow-name": "AdapterSmartSeq2SingleCell",
            "workflow-version": "smartseq2_v2.1.0"
        },
        "name": "AdapterSmartSeq2SingleCell",
        "start": "2019-01-06T20:18:18.102Z",
        "status": "Succeeded",
        "submission": "2019-01-06T20:16:30.804Z"
    }

    Among all of the fields, the labels is usually trivial and expensive to query for. So the default response from
    Secondary-analysis service (Cromwell API) will not contain the `labels` field unless the client is querying with
    specific parameters. This model is designed to comply with that rule, which means the `labels` is an optional
    property to this Workflow object, and will set to None is not provided. However, missing the `labels` field can
    cause the downstream functions depending on this field to run into errors.
    """

    def __init__(self, workflow_data: dict):
        self._load_data(workflow_data)

    def __eq__(self, other):
        return (isinstance(self, type(other)) and isinstance(other, type(self)) and (
            self.uuid, self.name, self.status, self.start_time, self.end_time) == (
                other.uuid, other.name, other.status, other.start_time, other.end_time))

    def __hash__(self):
        """Calculate the hash of a subset of properties.

        Also, in Python3 world, since key lookups are always followed by the equality check, we could care less
        about the hash collisions here.
        """
        return hash((self.uuid, self.name, self.status))

    def __str__(self, prefix: str = "", verbose: bool = False):

        workflow_info = colored(f"{prefix}Workflow {self.uuid}\n", 'green') + \
                                f"{prefix}    name={self.name}\n" \
                                f"{prefix}    status={self.status}\n"
        if verbose:
            workflow_info += f"{prefix}    submission_time={self.submission_time}\n" \
                             f"{prefix}    start_time={self.start_time}\n" \
                             f"{prefix}    end_time={self.end_time}\n"

        if self.labels:
            workflow_info += colored(f"{prefix}    labels: \n", 'blue')
            for label_key, label_value in self.labels.items():
                if label_value:
                    workflow_info += colored(f"{prefix}        {label_key}={label_value}\n")
        return workflow_info

    def __repr__(self):
        return self.uuid

    def _load_data(self, workflow_data: dict):
        assert isinstance(workflow_data, dict)
        self._uuid = workflow_data['id']
        self._name = workflow_data.get('name', '')
        self._status = workflow_data['status']
        self._start_time = workflow_data.get('start', '')
        self._end_time = workflow_data.get('end', '')
        self._submission_time = workflow_data.get('submission', '')

        if not workflow_data.get('labels'):
            self._labels = None
        else:
            self._labels = {
                'bundle-uuid': workflow_data['labels'].get('bundle-uuid', ''),
                'bundle-version': workflow_data['labels'].get('bundle-version', ''),
                'project_uuid': workflow_data['labels'].get('project_uuid', ''),
                'project_shortname': workflow_data['labels'].get('project_shortname', ''),
                'workflow-version': workflow_data['labels'].get('workflow-version', ''),
                'workflow-name': workflow_data['labels'].get('workflow-name', '')
            }

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    @property
    def status(self):
        return self._status

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def submission_time(self):
        return self._submission_time

    @property
    def labels(self):
        return self._labels

    def print(self, prefix: str = "", verbose: bool = False, associated_entities_to_show: str = None):
        print(self.__str__(prefix=prefix, verbose=verbose))
        if associated_entities_to_show:
            prefix = f"{prefix}    "

            # Show associated bundle
            # Will raise an error when the `labels` property is missing
            if any(keyword in associated_entities_to_show for keyword in ('bundles', 'bundle', 'all')):
                print(f"{prefix}Bundle:")
                print(prefix + "    " + self.labels.get('bundle-uuid'))

            # Show associated project
            # Will raise an error when the `labels` property is missing
            if any(keyword in associated_entities_to_show for keyword in ('projects', 'project', 'all')):
                print(f"{prefix}Project Shortname:")
                print(prefix + "    " + self.labels.get('project_shortname'))
                print(f"{prefix}Project UUID:")
                print(prefix + "    " + self.labels.get('project_uuid'))
