import sys
import re

for line in sys.stdin:
    try:
        doc_id, title, content = line.strip().split("\t")
        text = f"{title} {content}".lower()
        words = re.findall(r'\w+', text)

        print(f"!doclen\t{doc_id}\t{len(words)}")

        for word in words:
            print(f"{word}\t{doc_id}")
    except:
        continue
