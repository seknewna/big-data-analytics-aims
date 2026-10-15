"""Word count with minimr. Usage: python wordcount.py corpus.txt"""
import re
import sys
from minimr import map_reduce


def mapper(line):
    for w in re.findall(r"[a-zà-ÿ']+", line.lower()):
        yield w, 1


def reducer(word, counts):
    return sum(counts)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "corpus.txt"
    with open(path, encoding="utf-8") as f:
        result = map_reduce(f, mapper, reducer, combiner=reducer)
    for w, c in sorted(result, key=lambda x: -x[1])[:20]:
        print(f"{w:20s} {c}")
