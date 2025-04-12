#!/usr/bin/env python3

import sys
import re
import traceback

def tokenize(text):
    """
    Tokenize a text string into lowercase words using regex.
    Extendable: you can add stemming, stopword removal, etc.
    """
    return re.findall(r'\w+', text.lower())

def process_line(line):
    """
    Parse and process a single input line.
    Returns a tuple: (doc_id, tokens, doc_length) or None if invalid.
    """
    parts = line.strip().split("\t", 2)
    if len(parts) != 3:
        sys.stderr.write(f"[mapper1] Invalid line (expected 3 tab-separated parts): {line}\n")
        return None

    doc_id, title, content = parts
    full_text = f"{title} {content}"
    tokens = tokenize(full_text)

    return doc_id, tokens, len(tokens)

def emit(doc_id, tokens, doc_length):
    """
    Emit document length and term-document pairs.
    Output format is tab-separated, one per line.
    """
    print(f"!doclen\t{doc_id}\t{doc_length}")
    for token in tokens:
        print(f"{token}\t{doc_id}")

def main():
    for line in sys.stdin:
        if not line.strip():
            continue

        try:
            result = process_line(line)
            if result:
                doc_id, tokens, doc_length = result
                emit(doc_id, tokens, doc_length)
        except Exception as e:
            sys.stderr.write(f"[mapper1] Exception during processing: {e}\n")
            traceback.print_exc(file=sys.stderr)
            continue

if __name__ == "__main__":
    main()
