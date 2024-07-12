from typing import Iterable
from tqdm import tqdm


def tqdm_with_current(iterable: Iterable):
    pbar = tqdm(iterable)
    for current in pbar:
        pbar.set_description(current)
        yield current
