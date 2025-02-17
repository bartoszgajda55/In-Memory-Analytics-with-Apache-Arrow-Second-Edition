#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2024 Packt
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import psutil
import pyarrow as pa
import pyarrow.csv
import pyarrow.compute as pc
import pandas as pd
import pyarrow.parquet as pq
import gc

# disable the garbage collection to make it more consistent
gc.disable()

# initial memory usage in megabytes
memory_init = psutil.Process(os.getpid()).memory_info().rss >> 20

# use pandas to read the csv
col_pd_csv = pd.read_csv('yellow_tripdata_2015-01.csv', usecols=['total_amount'])['total_amount']
col_pd_csv.mean()
memory_pd_csv = psutil.Process(os.getpid()).memory_info().rss >> 20

# use pyarrow and read the file but only the column we want
col_pa_csv = pa.csv.read_csv('yellow_tripdata_2015-01.csv',                
                convert_options=pa.csv.ConvertOptions(
                    include_columns=['total_amount']))
pc.mean(col_pa_csv['total_amount'])
memory_pa_csv = psutil.Process(os.getpid()).memory_info().rss >> 20

# pyarrow.parquet read just the column we want and then convert to pandas
col_parquet = pq.read_table('yellow_tripdata_2015-01.parquet', columns=['total_amount'])
pc.mean(col_parquet['total_amount'])
memory_parquet = psutil.Process(os.getpid()).memory_info().rss >> 20

# read .arrow IPC file
with pa.OSFile('yellow_tripdata_2015-01.arrow', 'rb') as source:
     col_arrow_file = pa.ipc.open_file(source).read_all().column(
                              'total_amount')
pc.mean(col_arrow_file)     
memory_arrow = psutil.Process(os.getpid()).memory_info().rss >> 20


source = pa.memory_map('yellow_tripdata_2015-01.arrow', 'rb')
col_arrow_mmap = pa.ipc.RecordBatchFileReader(source).read_all().column(
              'total_amount')
pc.mean(col_arrow_mmap)
memory_mmapped = psutil.Process(os.getpid()).memory_info().rss >> 20

print('pandas:', memory_pd_csv - memory_init, ' MB')
print('pyarrow:', memory_pa_csv - memory_pd_csv, ' MB')
print('parquet col:', memory_parquet - memory_pa_csv, ' MB')
print('arrow ipc:', memory_arrow - memory_parquet, ' MB')
print('mmap zero-copy:', memory_mmapped - memory_arrow, ' MB')
