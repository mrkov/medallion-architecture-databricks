"""Fetch the pinned ODCS JSON schema and verify its published content."""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

VERSION = "v3.2.0"
SCHEMA_SHA256 = "edb41f33ec46e84780e99872ab2bd67f074959d2bf3e9c9fc54e61f8982b0d93"
SCHEMA_URL = (
    "https://raw.githubusercontent.com/bitol-io/open-data-contract-standard/"
    f"{VERSION}/schema/odcs-json-schema-latest.json"
)
ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "contracts" / "schema" / f"odcs-json-schema-{VERSION}.json"


def main() -> int:
    data = urllib.request.urlopen(SCHEMA_URL, timeout=30).read()  # noqa: S310
    actual = hashlib.sha256(data).hexdigest()
    if actual != SCHEMA_SHA256:
        print(f"ODCS schema checksum mismatch: expected {SCHEMA_SHA256}, got {actual}", file=sys.stderr)
        return 1
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    DESTINATION.write_bytes(data)
    print(f"Verified ODCS {VERSION} schema at {DESTINATION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

