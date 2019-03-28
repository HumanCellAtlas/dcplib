"""
Shared ETL (extract, transform, load) code for fetching and loading data from HCA DSS.

The main entry point is the DSSExtractor.extract() function, which takes optional transform and load callbacks that
will be invoked for each bundle fetched.

Example usage:

    from dcplib.etl import DSSExtractor

    def tf(*args, **kwargs):
        print("Transformer", args, kwargs)
        return "TEST"

    def ld(*args, **kwargs):
        print("Loader", args, kwargs)

    def fn(*args, **kwargs):
        print("Finalizer", args, kwargs)

    DSSExtractor(staging_directory=".").extract(transformer=tf, loader=ld, finalizer=fn)
"""

import os, sys, json, concurrent.futures, hashlib, logging, threading
from collections import defaultdict
from fnmatch import fnmatchcase

import hca
from ..networking import http
from hca.util import RetryPolicy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DSSExtractor:
    default_bundle_query = {'query': {'bool': {'must_not': {'term': {'admin_deleted': True}}}}}
    default_content_type_patterns = ['application/json; dcp-type="metadata*"']

    def __init__(self, staging_directory, content_type_patterns: list = None, filename_patterns: list = None,
                 dss_client: hca.dss.DSSClient = None):
        self.sd = staging_directory
        self.content_type_patterns = content_type_patterns or self.default_content_type_patterns
        self.filename_patterns = filename_patterns or []
        self.b2f = defaultdict(set)
        self.staged_bundles = []
        self._dss_client = dss_client or hca.dss.DSSClient()
        self._dss_swagger_url = None

    # concurrent.futures.ProcessPoolExecutor requires objects to be picklable.
    # hca.dss.DSSClient is unpicklable and is stubbed out here to preserve DSSExtractor's picklability.
    def __getstate__(self):
        state = dict(self.__dict__)
        state["_dss_swagger_url"] = self.dss_client.swagger_url
        state["_dss_client"] = None
        return state

    @property
    def dss_client(self):
        if self._dss_client is None:
            self._dss_client = hca.dss.DSSClient(swagger_url=self._dss_swagger_url)
        return self._dss_client

    def link_file(self, bundle_uuid, bundle_version, f):
        if not os.path.exists(f"{self.sd}/bundles/{bundle_uuid}.{bundle_version}/{f['name']}"):
            logger.debug("Linking fetched file %s/%s", bundle_uuid, f["name"])
            os.symlink(f"../../files/{f['uuid']}", f"{self.sd}/bundles/{bundle_uuid}.{bundle_version}/{f['name']}")

    def get_file(self, f, bundle_uuid, bundle_version, print_progress=True):
        logger.debug("[%s] Fetching %s:%s", threading.current_thread().getName(), bundle_uuid, f["name"])
        res = http.get(f"{self.dss_client.host}/files/{f['uuid']}", params={"replica": "aws", "version": f["version"]})
        res.raise_for_status()
        with open(f"{self.sd}/files/{f['uuid']}", "wb") as fh:
            fh.write(res.content)
        self.link_file(bundle_uuid, bundle_version, f)
        logger.debug("Wrote %s:%s", bundle_uuid, f["name"])
        if print_progress:
            sys.stdout.write(".")
            sys.stdout.flush()
        return f, bundle_uuid, bundle_version

    def should_fetch_file(self, f):
        if any(fnmatchcase(f["content-type"], p) for p in self.content_type_patterns):
            return True
        if any(fnmatchcase(f["name"], p) for p in self.filename_patterns):
            return True
        return False

    def get_files_to_fetch_for_bundle(self, bundle_uuid, bundle_version):
        logger.debug("[%s] Fetching manifest for bundle %s", threading.current_thread().getName(), bundle_uuid)
        res = http.get(f"{self.dss_client.host}/bundles/{bundle_uuid}", params={"replica": "aws"})
        res.raise_for_status()
        logger.debug("Scanning bundle %s", bundle_uuid)
        files_to_fetch = []
        os.makedirs(f"{self.sd}/bundle_manifests", exist_ok=True)
        with open(f"{self.sd}/bundle_manifests/{bundle_uuid}.{bundle_version}.json", "w") as fh:
            json.dump(res.json()["bundle"], fh)
        for f in res.json()["bundle"]["files"]:
            if self.should_fetch_file(f):
                os.makedirs(f"{self.sd}/bundles/{bundle_uuid}.{bundle_version}", exist_ok=True)
                try:
                    with open(f"{self.sd}/files/{f['uuid']}", "rb") as fh:
                        file_csum = hashlib.sha256(fh.read()).hexdigest()
                        if file_csum == f["sha256"]:
                            self.link_file(bundle_uuid, bundle_version, f)
                            continue
                except (FileNotFoundError, ):
                    pass
                files_to_fetch.append(f)
            else:
                logger.debug("Skipping file %s/%s (no filter match)", bundle_uuid, f["name"])
        return bundle_uuid, bundle_version, files_to_fetch

    def enqueue_bundle_manifests(self, percent_complete, bundles_complete, total_bundles, f2f_futures, ff_futures,
                                 executor):
        logger.info("[%d%%] [%d/%d] Processing bundle batch",
                    percent_complete, bundles_complete, total_bundles)
        for future in concurrent.futures.as_completed(f2f_futures):
            bundle_uuid, bundle_version, files_to_fetch = future.result()
            if files_to_fetch:
                logger.info("[%d%%] Fetching %d files for bundle %s @ %s",
                            percent_complete, len(files_to_fetch), bundle_uuid, bundle_version)
                for f in files_to_fetch:
                    ff_futures.append(executor.submit(self.get_file, f, bundle_uuid, bundle_version))
                    self.b2f[bundle_uuid + "." + bundle_version].add(f["name"])
            else:
                self.staged_bundles.append([bundle_uuid, bundle_version])
        return len(f2f_futures)

    def extract(self, query: dict = None, transformer: callable = None, loader: callable = None,
                finalizer: callable = None, max_workers=512, max_dispatchers=1,
                dispatch_executor_class: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor):
        if query is None:
            query = self.default_bundle_query
        os.makedirs(f"{self.sd}/files", exist_ok=True)
        os.makedirs(f"{self.sd}/bundles", exist_ok=True)
        f2f_futures, ff_futures = [], []
        bundles_complete, percent_complete = 0, -1
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            total_bundles = self.dss_client.post_search(es_query=query, replica="aws")["total_hits"]
            if total_bundles == 0:
                logger.error("No bundles found, nothing to do")
                return
            logger.info("Scanning %s bundles", total_bundles)
            for b in self.dss_client.post_search.iterate(es_query=query, replica="aws", per_page=500):
                bundle_uuid, bundle_version = b["bundle_fqid"].split(".", 1)
                f2f_futures.append(executor.submit(self.get_files_to_fetch_for_bundle, bundle_uuid, bundle_version))
                if len(f2f_futures) > max_workers * 2:
                    bundles_complete += self.enqueue_bundle_manifests(percent_complete, bundles_complete, total_bundles,
                                                                      f2f_futures, ff_futures, executor)
                    f2f_futures.clear()

                if int((bundles_complete / total_bundles) * 100) > percent_complete:
                    percent_complete = int((bundles_complete / total_bundles) * 100)
                    logger.info("%d%% complete (%d/%d bundles)", percent_complete, bundles_complete, total_bundles)
            bundles_complete += self.enqueue_bundle_manifests(percent_complete, bundles_complete, total_bundles,
                                                              f2f_futures, ff_futures, executor)

        with dispatch_executor_class(max_workers=max_dispatchers) as executor:
            dispatch_callbacks_futures = []
            for future in concurrent.futures.as_completed(ff_futures):
                f, bundle_uuid, bundle_version = future.result()
                self.b2f[bundle_uuid + "." + bundle_version].remove(f["name"])
                if len(self.b2f[bundle_uuid + "." + bundle_version]) == 0:
                    future = executor.submit(self.dispatch_callbacks, bundle_uuid, bundle_version, transformer, loader)
                    dispatch_callbacks_futures.append(future)
                else:
                    logger.debug("%s: %d files to go", bundle_uuid, len(self.b2f[bundle_uuid + "." + bundle_version]))
            for bundle_uuid, bundle_version in self.staged_bundles:
                future = executor.submit(self.dispatch_callbacks, bundle_uuid, bundle_version, transformer, loader)
                dispatch_callbacks_futures.append(future)
            for future in concurrent.futures.as_completed(dispatch_callbacks_futures):
                future.result()

        if finalizer is not None:
            finalizer(extractor=self)
        logger.info("Done (%d/%d bundles)", bundles_complete, total_bundles)

    def dispatch_callbacks(self, bundle_uuid, bundle_version, transformer, loader):
        bundle_path = f"{self.sd}/bundles/{bundle_uuid}.{bundle_version}"
        bundle_manifest_path = f"{self.sd}/bundle_manifests/{bundle_uuid}.{bundle_version}.json"
        if transformer is not None:
            tb = transformer(bundle_uuid=bundle_uuid, bundle_version=bundle_version, bundle_path=bundle_path,
                             bundle_manifest_path=bundle_manifest_path, extractor=self)
        if loader is not None:
            loader(extractor=self, transformer=transformer, bundle=tb)
