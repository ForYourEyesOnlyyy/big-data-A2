#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    _, title, text = parts
    tokens = tokenize(f"{title} {text}")
    for token in set(tokens):  # emit only one occurrence per term
        print(token)
