from pyarrow import csv

table = csv.read_csv("./sample_data/train.csv")
print(table.column(0).num_chunks)
