"""
(1, 1)
(1, 2)
(1, 3)
--> (1, 6)
(2, 3)
(2, 4)
--> (2, 7)

xDay 00:00-x+1Day 00:00 blob1d
14:00-16:00 blob2h
16:00-16:10 blob
16:10-16:20 blob

blob_interval: 10min, 1hours, ...

"""

class Meta:
    metric_id: int32
    granularity # 0 ms - no merge, raw timestamps; another 10 ms..., 1 sec, ...
    first_timestamp: int64
    merge_function
    max_size: int64
    max_time: int64
    is_aggregated: bool
    is_memory_only: bool
    ttl: int64

class Data:
    values
    timestamps
    counts
    mins
    maxs
    lasts

class Blob:
    meta: Meta
    data: Data

