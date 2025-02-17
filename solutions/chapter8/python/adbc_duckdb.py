#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2021 Packt
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

import pyarrow as pa
import pyarrow.parquet as pq
import adbc_driver_duckdb.dbapi

tbl = pq.read_table('../../sample_data/yellow_tripdata_2015-01.parquet')

with adbc_driver_duckdb.dbapi.connect('test.db') as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest('taxi_sample', tbl)
        cur.execute('select count(*) from taxi_sample')
        print(cur.fetchone())

        cur.execute('select * from taxi_sample')
        tbl2 = cur.fetch_arrow_table()
        print(tbl2.num_rows)
