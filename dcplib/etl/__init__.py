"""
Shared ETL (extract, transform, load) code for fetching and loading data from HCA DSS.

The main entry point is the extract() function, which takes optional transform and load callbacks that will be invoked
for each bundle fetched.

TODO:
- content-type globs
- filename globs
- s3 bucket to save cache tar to after
- whether to pass directory path, filehandles or file contents
"""

import os, sys, json, concurrent.futures, hashlib, logging, threading
from fnmatch import fnmatchcase

import hca
from ..networking import http
from hca.util import RetryPolicy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DSSExtractor:
    default_bundle_query = {'query': {'bool': {'must_not': {'term': {'admin_deleted': True}}}}}
    default_content_type_pattern = 'application/json; dcp-type="metadata*"'

    def __init__(self):
        self.dss_host = hca.dss.DSSClient().host

    def get_file(self, f, bundle_uuid, bundle_version, print_progress=True):
        logger.debug("[%s] Fetching %s:%s", threading.current_thread().getName(), bundle_uuid, f["name"])
        res = http.get(f"{self.dss_host}/files/{f['uuid']}", params={"replica": "aws", "version": f["version"]})
        res.raise_for_status()
        with open(f"files/{f['uuid']}", "wb") as fh:
            fh.write(res.content)
        if not os.path.exists(f"bundles/{bundle_uuid}.{bundle_version}/{f['name']}"):
            os.symlink(f"../../files/{f['uuid']}", f"bundles/{bundle_uuid}.{bundle_version}/{f['name']}")
        logger.debug("Wrote %s:%s", bundle_uuid, f["name"])
        if print_progress:
            sys.stdout.write(".")
            sys.stdout.flush()

    def get_files_to_fetch_for_bundle(self, bundle_uuid, bundle_version):
        logger.debug("[%s] Fetching manifest for bundle %s", threading.current_thread().getName(), bundle_uuid)
        res = http.get(f"{self.dss_host}/bundles/{bundle_uuid}", params={"replica": "aws"})
        res.raise_for_status()
        logger.debug("Scanning bundle %s", bundle_uuid)
        files_to_fetch = []
        for f in res.json()["bundle"]["files"]:
            if f["content-type"].startswith('application/json; dcp-type="metadata'):
                os.makedirs(f"bundles/{bundle_uuid}.{bundle_version}", exist_ok=True)
                try:
                    with open(f"files/{f['uuid']}", "rb") as fh:
                        file_csum = hashlib.sha256(fh.read()).hexdigest()
                        if file_csum == f["sha256"]:
                            logger.debug("Linking fetched file %s/%s", bundle_uuid, f["name"])
                            if not os.path.exists(f"bundles/{bundle_uuid}.{bundle_version}/{f['name']}"):
                                os.symlink(f"../../files/{f['uuid']}",
                                           f"bundles/{bundle_uuid}.{bundle_version}/{f['name']}")
                            continue
                except (FileNotFoundError, ):
                    pass
                files_to_fetch.append(f)
            else:
                logger.debug("Skipping non-metadata file %s/%s", bundle_uuid, f["name"])
        return bundle_uuid, bundle_version, files_to_fetch

    def extract(self, query: dict = None, transformer: callable = None, loader: callable = None,
                staging_directory: str = None, content_type_pattern=None, filename_pattern=None,
                max_workers=512):
        if query is None:
            query = self.default_bundle_query
        if content_type_pattern is None:
            content_type_pattern = self.default_content_type_pattern
        if staging_directory is None:
            staging_directory = "."
        os.chdir(staging_directory)
        os.makedirs("files", exist_ok=True)
        os.makedirs("bundles", exist_ok=True)
        f2f_futures, ff_futures = [], []
        bundles_complete, percent_complete = 0, -1
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            dss = hca.dss.DSSClient(swagger_url=f"{self.dss_host}/swagger.json")
            total_bundles = dss.post_search(es_query=query, replica="aws")["total_hits"]
            logging.info("Scanning %s bundles", total_bundles)
            for b in dss.post_search.iterate(es_query=query, replica="aws", per_page=500):
                bundle_uuid, bundle_version = b["bundle_fqid"].split(".", 1)
                f2f_futures.append(executor.submit(self.get_files_to_fetch_for_bundle, bundle_uuid, bundle_version))

                if len(f2f_futures) > max_workers * 2:
                    logger.info("[%d%%] [%d/%d] Processing bundle batch",
                                percent_complete, bundles_complete, total_bundles)
                    for future in concurrent.futures.as_completed(f2f_futures):
                        bundle_uuid, bundle_version, files_to_fetch = future.result()
                        if files_to_fetch:
                            logger.info("[%d%%] Fetching %d files for bundle %s @ %s",
                                        percent_complete, len(files_to_fetch), bundle_uuid, bundle_version)
                            for f in files_to_fetch:
                                ff_futures.append(executor.submit(self.get_file, f, bundle_uuid, bundle_version))
                    bundles_complete += len(f2f_futures)
                    f2f_futures.clear()

                if int((bundles_complete / total_bundles) * 100) > percent_complete:
                    percent_complete = int((bundles_complete / total_bundles) * 100)
                    logger.info("%d%% complete (%d/%d bundles)", percent_complete, bundles_complete, total_bundles)

        for f in concurrent.futures.as_completed(ff_futures):
            bundle = f.result()
            if transformer is not None:
                bundle = transformer(bundle)
            if loader is not None:
                loader(bundle)

        logger.info("Done (%d/%d bundles)", bundles_complete, total_bundles)
