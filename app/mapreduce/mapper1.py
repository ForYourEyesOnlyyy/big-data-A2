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
    parts = line.strip().split("\t", 2)
    if len(parts) != 3:
        sys.stderr.write(f"[mapper1] Skipping malformed line: {line}\n")
        return None
    doc_id, title, content = parts
    full_text = f"{title} {content}"
    tokens = tokenize(full_text)
    return doc_id, title, tokens, len(tokens)

def emit(doc_id, title, tokens, doc_length):
    print(f"!doclen\t{doc_id}\t{doc_length}")
    print(f"!title\t{doc_id}\t{title}")
    for token in tokens:
        print(f"{token}\t{doc_id}")


def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            result = process_line(line)
            if result:
                doc_id, title, tokens, doc_length = result
                emit(doc_id, title, tokens, doc_length)
        except Exception as e:
            sys.stderr.write(f"[mapper1] Error processing line: {e}\n")
            traceback.print_exc(file=sys.stderr)


if __name__ == "__main__":
    main()
