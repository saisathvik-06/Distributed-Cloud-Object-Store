"""Choosing which workers hold a chunk."""
import random


def choose_workers(candidates, load: dict[str, int], count: int) -> list[str]:
    """Pick up to `count` workers: least-loaded first, random tie-break.

    The random tie-break stops the same two workers from always being paired,
    which would concentrate risk on a fixed partner when one of them fails.
    """
    ranked = sorted(candidates, key=lambda w: (load.get(w, 0), random.random()))
    return ranked[:count]
