import os
import sys
import unittest
import json
from unittest import mock
from pathlib import Path
from dcplib.component_agents.analysis_agent import AnalysisAgent


data_dir = '{}/data/'.format(Path(os.path.split(__file__)[0]).absolute())


@unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
class TestAnalysisAgent(unittest.TestCase):
    def setUp(self):
        with open("{}workflow_response/simple_query_results.json".format(data_dir)) as f:
            self.simple_workflow_response = json.load(f)

        with open("{}workflow_response/complex_query_results.json".format(data_dir)) as f:
            self.complex_workflow_response = json.load(f)

    def test_analysis_agent_raises_exception_when_no_deployment_specified(self):
        self.assertRaises(KeyError, AnalysisAgent)

    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_analysis_agent_can_resolve_the_right_collection_for_integration_deployment(
        self
    ):
        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ):
            agent = AnalysisAgent()
            self.assertEqual(agent.cromwell_collection, 'lira-int')

    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_get_workflows_by_project_uuid_can_fetch_simple_workflow_results(self):
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=self.simple_workflow_response)

        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ), mock.patch('cromwell_tools.api.query', response):
            agent = AnalysisAgent()
            result = agent.get_workflows_by_project_uuid(
                project_uuid='fake-uuid', with_labels=False
            )
            self.assertEqual(
                len(result), self.simple_workflow_response['totalResultsCount']
            )

    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_get_workflows_by_project_uuid_can_fetch_complex_workflow_results(self):
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=self.complex_workflow_response)

        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ), mock.patch('cromwell_tools.api.query', response):
            agent = AnalysisAgent()
            result = agent.get_workflows_by_project_uuid(
                project_uuid='fake-uuid', with_labels=False
            )
            self.assertEqual(
                len(result), self.complex_workflow_response['totalResultsCount']
            )
            self.assertEqual(result[0].get('labels').get('project_uuid'), 'fake-uuid')

    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_get_workflows_by_workflow_uuid_can_fetch_simple_workflow_results(self):
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=self.simple_workflow_response)

        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ), mock.patch('cromwell_tools.api.query', response):
            agent = AnalysisAgent()
            result = agent.get_workflows_by_workflow_uuid(
                workflow_uuid='fake-uuid', with_labels=False
            )
            self.assertEqual(
                len(result), self.simple_workflow_response['totalResultsCount']
            )
    
    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_get_workflows_by_bundle_uuid_can_fetch_simple_workflow_results(self):
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=self.simple_workflow_response)

        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ), mock.patch('cromwell_tools.api.query', response):
            agent = AnalysisAgent()
            result = agent.get_workflows_by_bundle_uuid(
                bundle_uuid='fake-uuid', with_labels=False
            )
            self.assertEqual(
                len(result), self.simple_workflow_response['totalResultsCount']
            )
    
    @mock.patch.dict(os.environ, {'DEPLOYMENT_STAGE': 'integration'})
    def test_get_workflows_by_bundle_uuid_can_fetch_simple_workflow_results_with_bundle_version(self):
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=self.simple_workflow_response)

        with mock.patch(
            'dcplib.component_agents.analysis_agent.AnalysisAgent.analysis_gcp_creds',
            None,
        ), mock.patch('cromwell_tools.api.query', response):
            agent = AnalysisAgent()
            result = agent.get_workflows_by_bundle_uuid(
                bundle_uuid='fake-uuid', bundle_version='fake-version', with_labels=False
            )
            self.assertEqual(
                len(result), self.simple_workflow_response['totalResultsCount']
            )