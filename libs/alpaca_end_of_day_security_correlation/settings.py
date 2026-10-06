import re

MANIFEST_VERSION = 1
SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]*")

CORRELATED_BRANCH = "correlated"
UNCORRELATED_BRANCH = "uncorrelated"
