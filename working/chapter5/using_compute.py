import pyarrow.parquet as pq
import pyarrow.compute as pc

filepath = "./sample_data/yellow_tripdata_2015-01.parquet"
tbl = pq.read_table(filepath)
column = tbl["total_amount"]
# Adding scalar value
print(pc.add(column, 5.5))
# Min Max
print(pc.min_max(column))
# Sorting
sort_keys = [("total_amount", "descending")]
print(tbl.take(pc.sort_indices(tbl, sort_keys)))
