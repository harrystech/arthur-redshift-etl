"""
Determine how to split up large data sizes into smaller chunks.

Extractors often need to partition the input so that multiple smaller parts can be operated on in parallel.
"""
from typing import Tuple


def divide_by_twos(table_size: int, max_partitions: int) -> Tuple[int, int]:
    """
    Suggest number of partitions based on the table size (in bytes).  Number of partitions is always
    a factor of 2.

    The number of partitions is based on:
      Small tables (<= 10M): Use partitions around 1MB.
      Medium tables (<= 1G): Use partitions around 10MB.
      Huge tables (> 1G): Use partitions around 20MB.

    >>> divide_by_twos(100, 1024)
    (1, 100)
    >>> divide_by_twos(1048576, 1024)
    (1, 1048576)
    >>> divide_by_twos(3 * 1048576, 1024)
    (2, 1572864)
    >>> divide_by_twos(10 * 1048576, 1024)
    (8, 1310720)
    >>> divide_by_twos(100 * 1048576, 1024)
    (8, 13107200)
    >>> divide_by_twos(200 * 1048576, 1024)
    (16, 13107200)
    >>> divide_by_twos(2000 * 1048576, 1024)
    (64, 32768000)
    """
    meg = 1024 * 1024
    if table_size <= 10 * meg:
        target = 1 * meg
    elif table_size <= 1024 * meg:
        target = 10 * meg
    else:
        target = 20 * meg

    num_partitions = 1
    partition_size = table_size
    # Keep the partition sizes above the target value:
    while partition_size >= target * 2 and num_partitions < max_partitions:
        num_partitions *= 2
        partition_size //= 2

    return (num_partitions, partition_size)


def maximize_partitions(table_size: int, max_partitions: int, min_partition_size: int) -> Tuple[int, int]:
    """
    Determine the maximum number of row-wise partitions a table can be divided into while respecting a minimum partition
    size, and a limit on the number of partitions.

    Given a table size (in bytes), the maximum number of partitions to divide the table, and the minimum partition size
    (in bytes), return both the number of partitions and the calculated partition size.

    >>> maximize_partitions(1, 64, 1048576)
    (1, 1)
    >>> maximize_partitions(1048575, 64, 1048576)
    (1, 1048575)
    >>> maximize_partitions(1048576, 64, 1048576)
    (1, 1048576)
    >>> maximize_partitions(1048577, 64, 1048576)
    (1, 1048577)
    >>> maximize_partitions(2097151, 64, 1048576)
    (1, 2097151)
    >>> maximize_partitions(2097152, 64, 1048576)
    (2, 1048576)
    >>> maximize_partitions(67108863, 64, 1048576)
    (63, 1065220)
    >>> maximize_partitions(67108864, 64, 1048576)
    (64, 1048576)
    >>> maximize_partitions(67108865, 64, 1048576)
    (64, 1048576)
    >>> maximize_partitions(47095840768, 64, 1048576)
    (64, 735872512)
    >>> maximize_partitions(0, 64, 1048576)
    (1, 0)
    """
    partitions = max_partitions
    partition_size = table_size / partitions
    while partition_size < min_partition_size and partitions > 1:
        partitions -= 1
        partition_size = table_size / partitions

    return (partitions, int(partition_size))
