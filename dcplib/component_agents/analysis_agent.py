import base64
import logging
import os
import json

import cromwell_tools
from typing import List, Dict


class AnalysisAgent:
    """
    A collection of functions for working with Cromwell-as-a-Service APIs directly, 
    which is the workflow execution engine of Pipelines Execution Service.
    """

    def __init__(self) -> None:
        deployment = os.environ['DEPLOYMENT_STAGE']
        self.cromwell_url = 'https://cromwell.caas-prod.broadinstitute.org'
        self.cromwell_collection = (
            'lira-int' if deployment == 'integration' else f'lira-{deployment}'
        )
        self.auth = cromwell_tools.cromwell_auth.CromwellAuth.harmonize_credentials(
            service_account_key=self.analysis_gcp_creds, url=self.cromwell_url
        )

    @property
    def analysis_gcp_creds(self):
        # TODO: FIGURE OUT HOW TO PASS IN CREDS
        # json.loads(base64.b64decode(TrackerInfraConfig().analysis_gcp_creds).decode())
        return NotImplemented

    def _query_cromwell(self, query_dict: dict) -> List[Dict[str, str]]:
        """Helper function for querying workflows in Cromwell.

        :param query_dict: Dictionary specify the workflow query parameters.

        :return result: A list of workflow metadata blocks that matched the query.
        """
        # to only query within the collection that associated with the current deployment
        query_dict['label']['caas-collection-name'] = self.cromwell_collection

        response = cromwell_tools.api.query(
            query_dict=query_dict, auth=self.auth, raise_for_status=True
        )
        result = response.json()['results']
        return result

    def get_workflows_by_project_uuid(
        self, project_uuid: str, with_labels: bool = True
    ) -> List[Dict[str, str]]:
        """Query the workflows in Pipeline Execution Service (Cromwell) by project uuid.

        :param project_uuid: The UUID of an ingested HCA DCP data project.
        :param with_labels: Whether to include all workflow labels information in the result. Note
                            setting this to True will put some extra stress on the secondary analysis service
                            and might be slower to query.

        :return: A list of workflow metadata blocks that matched the query.
        """
        query_dict = {"label": {"project_uuid": project_uuid}}
        if with_labels:
            query_dict['additionalQueryResultFields'] = ['labels']
        return _query_cromwell(query_dict=query_dict)

    def get_workflows_by_workflow_uuid(
        self, workflow_uuid: str, with_labels: bool = True
    ) -> List[Dict[str, str]]:
        """Query the workflows in Pipeline Execution Service (Cromwell) by workflow uuid.

        :param workflow_uuid: The UUID of a analysis workflow in secondary analysis.
        :param with_labels: Whether to include all workflow labels information in the result. Note
                            setting this to True will put some extra stress on the secondary analysis service
                            and might be slower to query.

        :return: A list of workflow metadata blocks that matched the query.
        """
        query_dict = {'id': workflow_uuid}
        if with_labels:
            query_dict['additionalQueryResultFields'] = ['labels']
        return _query_cromwell(query_dict=query_dict)

    def get_workflows_by_bundle_uuid(
        self, bundle_uuid: str, bundle_version: str = None, with_labels: bool = True
    ) -> List[Dict[str, str]]:
        """Query the workflows in Pipeline Execution Service (Cromwell) by bundle (uuid and optionally version).

        :param bundle_uuid: The UUID of a bundle in Data Store.
        :param bundle_version: The version of a bundle in Data Store.
        :param with_labels: Whether to include all workflow labels information in the result. Note
                            setting this to True will put some extra stress on the secondary analysis service
                            and might be slower to query.

        :return: A list of workflow metadata blocks that matched the query.
        """
        query_dict = {'label': {'bundle-uuid': bundle_uuid}}
        if with_labels:
            query_dict['additionalQueryResultFields'] = ['labels']

        if bundle_version:
            query_dict['label']['bundle-version'] = bundle_version
        return _query_cromwell(query_dict=query_dict)
