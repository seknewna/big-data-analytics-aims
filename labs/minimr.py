"""A minimal MapReduce engine for teaching (Chapter 4). Same structure as Hadoop: map, combine, shuffle, reduce."""
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from itertools import chain
import hashlib


def _partition(key, n_reducers):
    """Deterministic hash partitioner (Python's hash() is randomised per process)."""
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % n_reducers


def _map_task(args):
    mapper, combiner, chunk, n_reducers = args
    buckets = [defaultdict(list) for _ in range(n_reducers)]
    for record in chunk:
        for k, v in mapper(record):
            buckets[_partition(k, n_reducers)][k].append(v)
    if combiner:
        buckets = [{k: [combiner(k, vs)] for k, vs in b.items()} for b in buckets]
    return buckets


def _reduce_task(args):
    reducer, groups = args
    return [(k, reducer(k, vs)) for k, vs in sorted(groups.items())]


def map_reduce(records, mapper, reducer, combiner=None, n_mappers=4, n_reducers=2):
    records = list(records)
    size = max(1, len(records) // n_mappers + 1)
    chunks = [records[i:i + size] for i in range(0, len(records), size)]
    with ProcessPoolExecutor(n_mappers) as ex:
        map_out = list(ex.map(_map_task, [(mapper, combiner, c, n_reducers) for c in chunks]))
        shuffled = []
        for r in range(n_reducers):
            merged = defaultdict(list)
            for buckets in map_out:
                for k, vs in buckets[r].items():
                    merged[k].extend(vs)
            shuffled.append(merged)
        red_out = list(ex.map(_reduce_task, [(reducer, g) for g in shuffled]))
    return list(chain.from_iterable(red_out))
