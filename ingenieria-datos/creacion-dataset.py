
#%%
import duckdb
import pandas as pd
from pathlib import Path

root_path = Path(__file__).parent.parent.resolve()

root_path = root_path.as_posix() + '/'


# Connect to DuckDB
con = duckdb.connect(database= root_path+'duckdb/competencia_01.duckdb')

# Read CSV into DuckDB
data_path = 'datasets/competencia_01_crudo/*/*.parquet'

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_crudo AS
    SELECT * FROM read_parquet('{root_path+data_path}', hive_partitioning = true)
""")

#%%
# Create the competencia_01 table
query = """
create or replace table competencia_01 as
with periodos as (
  select distinct foto_mes from competencia_01_crudo
), clientes as (
  select distinct numero_de_cliente from competencia_01_crudo
), todo as (
  select numero_de_cliente, foto_mes from clientes cross join periodos
), clase_ternaria as (
  select
  c.*
    , if(c.numero_de_cliente is null, 0, 1) as mes_0
  , lead(mes_0, 1) over (partition by t.numero_de_cliente order by foto_mes) as mes_1
  , lead(mes_0, 2) over (partition by t.numero_de_cliente order by foto_mes) as mes_2
  , if (mes_2 = 1, 'CONTINUA',
          if (mes_1 = 1 and mes_2 = 0, 'BAJA+2',
              if (mes_1 = 0 and mes_2 = 0, 'BAJA+1', null))) as clase_ternaria
  from todo t
  left join competencia_01_crudo c using (numero_de_cliente, foto_mes)
) select
* EXCLUDE (mes_0, mes_1, mes_2)
from clase_ternaria
where mes_0 = 1
"""
con.execute(query)

# Count the cases by clase_ternaria
conteo_query = """
SELECT COUNT(clase_ternaria) AS casos, clase_ternaria
FROM competencia_01
GROUP BY clase_ternaria
"""
conteo_result = con.execute(conteo_query).fetchdf()
print(conteo_result)

# Close the connection
con.close()