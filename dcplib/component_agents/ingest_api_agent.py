from .hal_agent import HALAgent


class IngestApiAgent(HALAgent):

    def __init__(self, deployment):
        self.deployment = deployment
        super().__init__(api_url_base=self._ingest_api_url())

    def _ingest_api_url(self):
        if self.deployment == 'prod':
            return "https://api.ingest.data.humancellatlas.org"
        else:
            return "https://api.ingest.{deployment}.data.humancellatlas.org".format(deployment=self.deployment)
