Components to be Shared by DCP Infrastructure Automation Directly via Github
============================================================================

This folder is not included in the dcplib Python package and is a place to centralize files
so that DCP infrastructure can fetch them.

Trufflehog Regex Patterns
-------------------------

`trufflehog_regex_patterns.json` is a set of custom regex patterns for use with trufflehog.

In a CI/CD pipeline, one can use the following script:

    wget -O regex.json https://raw.githubusercontent.com/HumanCellAtlas/dcplib/master/components/trufflehog_regex_patterns.json
    trufflehog --regex --rules regex.json --entropy=False https://github.com/HumanCellAtlas/data-store.git
    rm regex.json

This will check the entire git history of a repository for the patterns inside of `trufflehog_regex_patterns.json`, 
where in this case the example repository is: https://github.com/HumanCellAtlas/data-store.git

Notes:
1. `--entropy=False` turns off high entropy string matching, otherwise any uuid seems to trigger it.
2. This regexes.json is largely based off of [truffleHog's default regexes.json](https://github.com/dxa4481/truffleHogRegexes/blob/master/truffleHogRegexes/regexes.json).

For more information on truffleHog: https://github.com/dxa4481/truffleHog/
