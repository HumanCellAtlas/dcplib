import pytest
import os
import json
from pathlib import Path
from dcplib.component_agents.analysis_agent import AnalysisAgent
import cromwell_tools
from unittest import mock

data_dir = f'{Path(os.path.split(__file__)[0]).absolute()}/data/'


@pytest.fixture(scope='module')
def simple_workflow_response():
    with open(f"{data_dir}workflow_response/simple_query_results.json") as f:
        simple_workflow_response = json.load(f)
    return simple_workflow_response


@pytest.fixture(scope='module')
def complex_workflow_response():
    with open(f"{data_dir}workflow_response/complex_query_results.json") as f:
        complex_workflow_response = json.load(f)
    return complex_workflow_response


@pytest.fixture(scope='module')
def deployments():
    return ('staging', 'dev', 'prod')


@pytest.fixture(scope='module')
def test_config():
    """Force using None for the service account to avoid extra mocks"""
    return None


class TestAnalysisAgent(object):
    def test_analysis_agent_raises_exception_when_no_deployment_specified(self, monkeypatch):
        monkeypatch.delenv('DEPLOYMENT_STAGE', raising=False)
        with pytest.raises(KeyError):
            agent = AnalysisAgent()
            assert agent

    def test_analysis_agent_can_resolve_the_right_collection_for_integration_deployment(self, monkeypatch, test_config):
        monkeypatch.setenv('DEPLOYMENT_STAGE', 'integration')
        monkeypatch.setattr(AnalysisAgent, 'analysis_gcp_creds', test_config)
        agent = AnalysisAgent()
        assert agent.cromwell_collection == 'lira-int'

    def test_analysis_agent_can_resolve_the_right_collection_for_non_integration_deployments(self, monkeypatch, deployments, test_config):
        for deployment in deployments:
            monkeypatch.setenv('DEPLOYMENT_STAGE', deployment)
            monkeypatch.setattr(AnalysisAgent, 'analysis_gcp_creds', test_config)
            agent = AnalysisAgent()
            assert agent.cromwell_collection == f'lira-{deployment}'

    def test_get_workflows_for_project_uuid_can_fetch_simple_workflow_results(self, monkeypatch, simple_workflow_response, test_config):
        monkeypatch.setenv('DEPLOYMENT_STAGE', 'integration')
        monkeypatch.setattr(AnalysisAgent, 'analysis_gcp_creds', test_config)
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=simple_workflow_response)
        monkeypatch.setattr('cromwell_tools.api.query', response)

        agent = AnalysisAgent()
        result = agent.get_workflows_for_project_uuid(project_uuid='fake-uuid', with_labels=False)
        assert len(result) == simple_workflow_response['totalResultsCount']

    def test_get_workflows_for_project_uuid_can_fetch_complex_workflow_results(self, monkeypatch, complex_workflow_response, test_config):
        monkeypatch.setenv('DEPLOYMENT_STAGE', 'integration')
        monkeypatch.setattr(AnalysisAgent, 'analysis_gcp_creds', test_config)
        response = mock.MagicMock
        response.json = mock.MagicMock(return_value=complex_workflow_response)
        monkeypatch.setattr('cromwell_tools.api.query', response)

        agent = AnalysisAgent()
        result = agent.get_workflows_for_project_uuid(project_uuid='fake-uuid', with_labels=False)
        assert len(result) == complex_workflow_response['totalResultsCount']
        assert result[0].get('labels') is not None
        assert result[0].get('labels').get('project_uuid') == 'fake-uuid'

    # TODO: rex: add more test cases
