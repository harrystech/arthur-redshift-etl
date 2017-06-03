"""
Determine how to split up large data sizes into smaller chunks.

Extractors often need to partition the input so that multiple smaller parts can be operated on in parallel.
"""


class DefaultPartitioningStrategy:
    """
    Suggest number of partitions based on the table size (in bytes).  Number of partitions is always
    a factor of 2.

    The number of partitions is based on:
      Small tables (<= 10M): Use partitions around 1MB.
      Medium tables (<= 1G): Use partitions around 10MB.
      Huge tables (> 1G): Use partitions around 20MB.

    >>> DefaultPartitioningStrategy(100, 1024).num_partitions()
    1
    >>> DefaultPartitioningStrategy(1048576, 1024).num_partitions()
    1
    >>> DefaultPartitioningStrategy(3 * 1048576, 1024).num_partitions()
    2
    >>> DefaultPartitioningStrategy(10 * 1048576, 1024).num_partitions()
    8
    >>> DefaultPartitioningStrategy(100 * 1048576, 1024).num_partitions()
    8
    >>> DefaultPartitioningStrategy(200 * 1048576, 1024).num_partitions()
    16
    >>> DefaultPartitioningStrategy(2000 * 1048576, 1024).num_partitions()
    64
    """

    def __init__(self, table_size: int, max_partitions: int) -> None:
        self.table_size = table_size
        self.max_partitions = max_partitions

    def num_partitions(self) -> int:
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
        while partition_size >= target * 2 and num_partitions < 1024:
            num_partitions *= 2
            partition_size //= 2

        return num_partitions
