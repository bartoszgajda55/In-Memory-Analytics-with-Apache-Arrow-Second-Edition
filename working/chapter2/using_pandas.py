import pandas as pd
import pyarrow as pa

df = pd.DataFrame({"a": [1, 2, 3]})
table = pa.Table.from_pandas(df)
print(table)
