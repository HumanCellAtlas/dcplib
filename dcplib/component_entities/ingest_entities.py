import json

from termcolor import colored

from dcplib.component_entities import EntityBase


class Project(EntityBase):
    """
    Model an Ingest Project entity
    """

    @classmethod
    def load_by_id(cls, project_id, ingest_api_agent):
        data = ingest_api_agent.get(f"/projects/{project_id}")
        return Project(project_data=data, ingest_api_agent=ingest_api_agent)

    @classmethod
    def load_by_uuid(cls, project_uuid, ingest_api_agent):
        data = ingest_api_agent.get(f"/projects/search/findByUuid?uuid={project_uuid}")
        return Project(project_data=data, ingest_api_agent=ingest_api_agent)

    def __init__(self, project_data=None, ingest_api_agent=None):
        self.api = ingest_api_agent
        self.data = project_data

    def __str__(self, prefix="", verbose=False):
        return colored(f"{prefix}Project {self.id}\n", 'green') + \
            f"{prefix}    uuid={self.uuid}\n" \
            f"{prefix}    short_name={self.short_name}\n"

    @property
    def id(self):
        return self.data['_links']['self']['href'].split('/')[-1]

    @property
    def uuid(self):
        return self.data['uuid']['uuid']

    @property
    def short_name(self):
        project_core = self.data['content']['project_core']
        return project_core.get('project_short_name') or project_core.get('project_shortname')

    @property
    def title(self):
        return self.data['content']['project_core']['project_title']

    @property
    def primary_investigator(self):
        primary_investigator = ''
        for name, role in self._iter_contributors():
            if role == "principal investigator":
                primary_investigator += f"{name}; "
        return 'N/A' if primary_investigator == '' else primary_investigator

    @property
    def data_curator(self):
        data_curator = ''
        for name, role in self._iter_contributors():
            if role == "data curator" or role == "Human Cell Atlas wrangler":
                data_curator += f"{name}; "
        return 'N/A' if data_curator == '' else data_curator

    """
    Yields [name, role]
    for contributors that have a role
    """
    def _iter_contributors(self):
        for contributor in self.data['content']['contributors']:
            project_role = contributor.get('project_role')
            if project_role:
                if contributor.get('contact_name'):
                    name = contributor['contact_name']
                else:
                    name = contributor['name']

                if type(project_role) == str:
                    role_ontology_label = project_role
                else:
                    role_ontology_label = project_role.get('ontology_label')

                yield name, role_ontology_label

    def print(self, prefix="", verbose=False, associated_entities_to_show=None):
        print(self.__str__(prefix=prefix, verbose=verbose))
        if associated_entities_to_show:
            prefix = f"{prefix}\t"
            if 'submissions' in associated_entities_to_show or 'all' in associated_entities_to_show:
                for subm in self.submission_envelopes():
                    subm.print(prefix=prefix, verbose=verbose, associated_entities_to_show=associated_entities_to_show)

    def submission_envelopes(self):
        data = self.api.get(self.data['_links']['submissionEnvelopes']['href'])
        return [
            SubmissionEnvelope(submission_data=subm_data, ingest_api_agent=self.api)
            for subm_data in data['_embedded']['submissionEnvelopes']
        ]


class Biomaterials(EntityBase):

    def __init__(self, biomaterial_data=None, ingest_api_agent=None):
        self.api = ingest_api_agent
        self.data = biomaterial_data

    @property
    def species(self):
        """
        :returns: an array of species
        :rtype: list
        """
        all_species = []
        if self.data['content'].get('genus_species'):
            genus_species = self.data['content']['genus_species']
            for species in genus_species:
                if species.get('ontology_label'):
                    all_species.append(species['ontology_label'])
                elif species.get('text'):
                    all_species.append(species['text'])
        return all_species


class Protocol(EntityBase):

    def __init__(self, protocol_data=None, ingest_api_agent=None):
        self.api = ingest_api_agent
        self.data = protocol_data

    @property
    def library_construction_method(self):
        """
        :return: library construction method or None
        :rtype: str or None
        """
        content = self.data['content']
        method = content.get('library_construction_method') or content.get('library_construction_approach')
        if method:
            return method.get('ontology_label') or method.get('text')
        else:
            return None


class Process(EntityBase):

    def __init__(self, process_data=None, ingest_api_agent=None):
        self.api = ingest_api_agent
        self.data = process_data

    @property
    def input_bundles(self):
        """
        :return: input_bundles
        :rtype: list
        """
        content = self.data['content']
        input_bundles = content.get('input_bundles', [])
        return input_bundles


class SubmissionEnvelope(EntityBase):
    """
    Model an Ingest Submission Envelope entity
    """

    UNPROCESSED_STATUSES = ['Invalid', 'Draft', 'Valid', 'Pending', 'Validating']

    @classmethod
    def load_by_id(cls, submission_id, ingest_api_agent):
        data = ingest_api_agent.get(f"/submissionEnvelopes/{submission_id}")
        return SubmissionEnvelope(submission_data=data, ingest_api_agent=ingest_api_agent)

    @classmethod
    def iter_submissions(cls, ingest_api_agent, page_size=500, sort_by='submissionDate,desc'):
        for page in ingest_api_agent.iter_pages('/submissionEnvelopes', page_size=page_size, sort=sort_by):
            for submission_data in page['submissionEnvelopes']:
                yield SubmissionEnvelope(submission_data=submission_data, ingest_api_agent=ingest_api_agent)

    def __init__(self, submission_data, ingest_api_agent):
        self.data = submission_data
        self.api = ingest_api_agent
        self.envelope_id = self.data['_links']['self']['href'].split('/')[-1]

    def __str__(self, prefix="", verbose=False):
        return colored(f"{prefix}SubmissionEnvelope {self.envelope_id}\n", 'green') + \
            f"{prefix}    uuid={self.uuid}\n" \
            f"{prefix}    status={self.status}"

    def print(self, prefix="", verbose=False, associated_entities_to_show=None):
        print(self.__str__(prefix=prefix, verbose=verbose))
        if associated_entities_to_show:
            prefix = f"{prefix}    "
            if 'bundles' in associated_entities_to_show or 'all' in associated_entities_to_show:
                if self.status == 'Complete':
                    print(colored(f"{prefix}Bundles:", 'cyan'))
                    for bundle in self.bundles():
                        print(prefix + "    " + bundle)

            if 'files' in associated_entities_to_show or 'all' in associated_entities_to_show:
                print(colored(f"{prefix}Files:", 'cyan'))
                for file in self.files():
                    file.print(prefix=prefix, verbose=verbose,
                               associated_entities_to_show=associated_entities_to_show)

    @property
    def submission_id(self):
        return self.data['_links']['self']['href'].split('/')[-1]

    @property
    def status(self):
        return self.data['submissionState']

    @property
    def submission_date(self):
        return self.data['submissionDate']

    @property
    def uuid(self):
        return self.data['uuid']['uuid']

    @property
    def is_unprocessed(self):
        return self.status in self.UNPROCESSED_STATUSES

    def files(self):
        return [File(file_data) for file_data in
                self.api.get_all(self.data['_links']['files']['href'], 'files')]

    def iter_files(self):
        url = self.data['_links']['files']['href']
        for page in self.api.iter_pages(url):
            for file in page['files']:
                yield file

    def projects(self):
        return [Project(project_data=proj_data, ingest_api_agent=self.api) for proj_data in
                self.api.get_all(self.data['_links']['projects']['href'], 'projects')]

    def biomaterials(self):
        return [Biomaterials(biomaterial_data, ingest_api_agent=self.api) for biomaterial_data in
                self.api.get_all(self.data['_links']['biomaterials']['href'], 'biomaterials', page_size=20)]

    def protocols(self):
        return [Protocol(protocol_data, ingest_api_agent=self.api) for protocol_data in
                self.api.get_all(self.data['_links']['protocols']['href'], 'protocols', page_size=1000)]

    def processes(self):
        return [Process(process_data, ingest_api_agent=self.api) for process_data in
                self.api.get_all(self.data['_links']['processes']['href'], 'processes', page_size=20)]

    def project(self):
        """ Assumes only one project """
        projects = self.projects()
        if len(projects) != 1:
            raise RuntimeError(f"Expect 1 project got {len(projects)}")
        return projects[0]

    def upload_credentials(self):
        """ Return upload area credentials or None if this envelope doesn't have an upload area yet """
        staging_details = self.data.get('stagingDetails', None)
        if staging_details and 'stagingAreaLocation' in staging_details:
            return staging_details.get('stagingAreaLocation', {}).get('value', None)
        return None

    def bundle_count(self):
        """An optimized version of bundle counting, using the fact that the HAL API returns
        'totalElements' in the first response.
        """
        bundle_manifest_url = self.data['_links']['bundleManifests']['href']
        data = self.api.get(bundle_manifest_url)
        return data['page']['totalElements']

    def bundles(self):
        url = self.data['_links']['bundleManifests']['href']
        manifests = self.api.get_all(url, 'bundleManifests')
        return [manifest['bundleUuid'] for manifest in manifests]


class File:
    """
    Model an Ingest File entity
    """

    def __init__(self, file_data):
        self._data = file_data

    def __str__(self, prefix="", verbose=False):
        return (f"{prefix}    fileName {self.name}\n"
                f"{prefix}    cloudUrl {self.cloud_url}\n")

    def __repr__(self):
        return json.dumps(self._data, indent=2)

    @property
    def name(self):
        return self._data['fileName']

    @property
    def cloud_url(self):
        return self._data['cloudUrl']

    def print(self, prefix="", verbose=False, associated_entities_to_show=None):
        print(self.__str__(prefix=prefix, verbose=verbose))
