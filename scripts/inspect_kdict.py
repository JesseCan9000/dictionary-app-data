#!/usr/bin/env python3
"""Quick local inspector for compiled `.kdict` files."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from update_dictionaries import lookup_in_kdict


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python3 scripts/inspect_kdict.py <path/to/code.kdict> <word> [word2 ...]")
        return 1

    kdict_path = Path(sys.argv[1])
    for query in sys.argv[2:]:
        res = lookup_in_kdict(kdict_path, query)
        print(f"\n=== {query} ===")
        if res is None:
            print("No result")
        else:
            print(json.dumps(res, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
