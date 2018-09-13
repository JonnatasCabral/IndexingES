from itertools import islice, chain

def chunker(iterable, n):
    iterable = iter(iterable)
    while True:
        x = tuple(islice(iterable, n))
        if not x:
            return
        yield x