# Changes for v3.8.0 (2019-09-20)
Update trufflehog regex. (#56)
Update trufflehog patterns. (#55)
add processes to ingest entities (#53)

# Changes for v3.7.0 (2019-09-13)

# Changes for v3.6.0 (2019-09-13)

# Changes for v3.5.0 (2019-09-13)

# Changes for v3.4.0 (2019-09-13)
include envelopes with submitted status as in process (#52)

# Changes for v3.3.0 (2019-08-22)
reduce biomaterials end point page size to 20 for ingest api as larger page sizes lead to frequent timeouts (#51)
Authn  JWT helpers (#45)
Close handle on ChecksummingBufferedReader exit
add test for empty bundles (#33)

# Changes for v3.2.1 (2019-08-15)
metadata project_shortname has been replaced by project_short_name,
make ingest_entities.Project work with both.

# Changes for v3.2.0 (2019-08-14)
Added IngestApiAgent and ingest_entities (SubmissionEnvelope etc...)
which are used by data-monitoring-dashboard, dcp-diag and soon the dcp
integration tests.

# Changes for v3.1.0 (2019-08-08)

- ETL: add page_processor and page_size args to extract function (#46)

- Drop support for Python 3.4

# Changes for v3.0.0 (2019-07-30)
Drop support for Python 2.7, add 3.7

# Changes for v2.1.2 (2019-07-26)

- dcpquery.etl.DSSExtractor.get_file is public

# Changes for v2.1.1 (2019-07-25)

- ETL: make retry-after behavior configurable; improve logging (#36)

# Changes for v2.1.0 (2019-07-24)

- ETL: fetch all pages for GET bundle (#39)

- AwsSecret: add debug prints; add methods exists_in_aws and is_deleted

- Refactor ETL (DSSExtractor) (#28)

- Test fixes

# Changes for v2.0.3 (2019-05-30)

- Add SQS batch context manager (#26)

# Changes for v2.0.2 (2019-05-17)

- Fix erroneous initialization of ETL DSS client

# Changes for v2.0.1 (2019-05-17)

- Fix redirect logic when Retry-After is given with a 301 redirect (#23)

- ETL fixes: Configurable dispatch for empty bundles (#18); do not configure global logging

# Changes for v2.0.0 (2019-04-26)
Fix release
# Changes for v1.1.0 (2019-04-26)

# Changes for v1.10.0 (2019-04-26)
Remove gpg signing from twine
# Changes for v1.9.0 (2019-04-26)

# Changes for v1.8.0 (2019-04-26)
Added twine to requirements-dev
# Changes for v1.7.0 (2019-04-26)
Adding module for working with SQS. (#20)
# Changes for v1.6.6 (2019-04-25)

- networking.HTTPRequest: set max redirects to a high number (#19)

# Changes for v1.6.5 (2019-04-01)

- Add padding for crc32c calculation to make it 8 characters in length to be backwards compatible. (#17)

# Changes for v1.6.4 (2019-03-28)

- Removing crcmod dependency from dcplib (#16)

- ETL: consume dispatch callbacks futures, add tests (#15)

# Changes for v1.6.3 (2019-03-26)

- Add test helpers originally from DSS (#11)

# Changes for v1.6.2 (2019-03-22)

- ETL: Pass bundle manifest to transformer (#14)

# Changes for v1.6.1 (2019-03-22)

- add ETL option to parallelize transformers and loaders (#13)

- Make dcplib.etl.DSSExtractor picklable (#12)

- Add trufflehog regexes to dcplib. (#10)

# Changes for v1.6.0 (2019-03-21)

- Add AWS utilities used by DSS and Query Service (#9)

- Switch to the ICRAR implementation of CRC32C (#6)

- Pin puremagic dependency to version 1.4

# Changes for v1.5.1 (2019-03-11)

- Ignore Python 3.6 code when testing on lower versions of Python

- Fix directory initialization bug in ETL

# Changes for v1.5.0 (2019-02-22)

- Shared DSS client ETL code (#4)

- Add dcplib.networking.http (#5)

# Changes for v1.4.0 (2018-08-15)
Config: allow customization of secret name

# Changes for v1.3.2 (2018-08-01)
* S3Etag is now optionally configured with chunk size
* ChecksummingBufferedReader is now optionally configured with chunk size

# Changes for v1.3.1 (2018-07-31)
Add s3_multipart.MULTIPART_THRESHOLD and tests for get_s3_multipart_chunk_size()

# Changes for v1.3.0 (2018-07-31)
Add get_s3_multipart_chunk_size method

# Changes for v1.2.1 (2018-05-24)
Python 2.7 compatiility

# Changes for v1.2.0 (2018-05-24)
Add Config and AwsSecret classes

# Changes for v1.1.0 (2018-02-07)
Fold checksumming_io into dcplib.

# Changes for v1.0.1 (2017-11-10)
Retry.

# Changes for v1.0.0 (2017-11-10)
MediaType and DcpMediaType classes.

