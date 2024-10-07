import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd

df = pd.read_parquet('datasets/competencia_01_aum.parquet')


tabla = pa.Table.from_pandas(df)

part = ds.partitioning(
    pa.schema([("foto_mes", pa.int32())]), flavor="hive"
)

# write df as a parquet dataset
ds.write_dataset(tabla, base_dir = "datasets/competencia_01_aum",
                 format="parquet",
                 partitioning= part,
                 max_rows_per_file = 42000, max_rows_per_group = 42000,
                 existing_data_behavior = "overwrite_or_ignore")