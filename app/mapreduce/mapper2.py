#!/usr/bin/env python3

import sys
import traceback

def parse_line(line):
    """
    Expects: term \t doc_id \t tf
    Returns: (term, doc_id, tf) or None
    """
    parts = line.strip().split("\t")
    if len(parts) != 3:
        sys.stderr.write(f"[mapper2] Malformed line: {line}\n")
        return None
    term, doc_id, tf = parts
    return term, doc_id, tf

def emit(term, doc_id, tf):
    print(f"{term}\t{doc_id}\t{tf}")

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            result = parse_line(line)
            if result:
                term, doc_id, tf = result
                emit(term, doc_id, tf)
        except Exception as e:
            sys.stderr.write(f"[mapper2] Error: {e}\n")
            traceback.print_exc(file=sys.stderr)

if __name__ == "__main__":
    main()
