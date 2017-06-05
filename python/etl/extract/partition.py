"""
Determine how to split up large data sizes into smaller chunks.

Extractors often need to partition the input so that multiple smaller parts can be operated on in parallel.
"""
from typing import Tuple


class DefaultPartitioningStrategy:
    """
    Suggest number of partitions based on the table size (in bytes).  Number of partitions is always
    a factor of 2.

    The number of partitions is based on:
      Small tables (<= 10M): Use partitions around 1MB.
      Medium tables (<= 1G): Use partitions around 10MB.
      Huge tables (> 1G): Use partitions around 20MB.

    >>> DefaultPartitioningStrategy(100, 1024).calculate()
    1
    >>> DefaultPartitioningStrategy(1048576, 1024).calculate()
    1
    >>> DefaultPartitioningStrategy(3 * 1048576, 1024).calculate()
    2
    >>> DefaultPartitioningStrategy(10 * 1048576, 1024).calculate()
    8
    >>> DefaultPartitioningStrategy(100 * 1048576, 1024).calculate()
    8
    >>> DefaultPartitioningStrategy(200 * 1048576, 1024).calculate()
    16
    >>> DefaultPartitioningStrategy(2000 * 1048576, 1024).calculate()
    64
    """

    def __init__(self, table_size: int, max_partitions: int) -> None:
        self.table_size = table_size
        self.max_partitions = max_partitions

    def calculate(self) -> int:
        meg = 1024 * 1024
        if self.table_size <= 10 * meg:
            target = 1 * meg
        elif self.table_size <= 1024 * meg:
            target = 10 * meg
        else:
            target = 20 * meg

        num_partitions = 1
        partition_size = self.table_size
        # Keep the partition sizes above the target value:
        while partition_size >= target * 2 and num_partitions < self.max_partitions:
            num_partitions *= 2
            partition_size //= 2

        return num_partitions


class MaximizePartitionCountStrategy:
    """
    Determine the maximum number of row-wise partitions a table can be divided into while respecting a minimum partition
    size, and a limit on the number of partitions.

    Given a table size (in bytes), the maximum number of partitions to divide the table, and the minimum partition size
    (in bytes), return both the number of partitions and the calculated partition size.

    >>> MaximizePartitionCountStrategy(1, 64, 1048576).calculate()
    (1, 1)
    >>> MaximizePartitionCountStrategy(1048575, 64, 1048576).calculate()
    (1, 1048575)
    >>> MaximizePartitionCountStrategy(1048576, 64, 1048576).calculate()
    (1, 1048576)
    >>> MaximizePartitionCountStrategy(1048577, 64, 1048576).calculate()
    (1, 1048577)
    >>> MaximizePartitionCountStrategy(2097151, 64, 1048576).calculate()
    (1, 2097151)
    >>> MaximizePartitionCountStrategy(2097152, 64, 1048576).calculate()
    (2, 1048576)
    >>> MaximizePartitionCountStrategy(67108863, 64, 1048576).calculate()
    (63, 1065220)
    >>> MaximizePartitionCountStrategy(67108864, 64, 1048576).calculate()
    (64, 1048576)
    >>> MaximizePartitionCountStrategy(67108865, 64, 1048576).calculate()
    (64, 1048576)
    >>> MaximizePartitionCountStrategy(47095840768, 64, 1048576).calculate()
    (64, 735872512)
    >>> MaximizePartitionCountStrategy(0, 64, 1048576).calculate()
    (1, 0)
    """
    def __init__(self, table_size: int, max_partitions: int, min_partition_size: int) -> None:
        self.table_size = table_size
        self.max_partitions = max_partitions
        self.min_partition_size = min_partition_size

    def calculate(self) -> Tuple[int, int]:
        partitions = self.max_partitions
        partition_size = self.table_size / partitions
        while partition_size < self.min_partition_size and partitions > 1:
            partitions -= 1
            partition_size = self.table_size / partitions

        return (partitions, int(partition_size))
