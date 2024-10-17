#%%
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import re

root_path = Path(__file__).parent.parent.resolve()

root_path = root_path.as_posix() + '/'

# Connect to DuckDB database
con = duckdb.connect(root_path+'duckdb/competencia_01.duckdb')

# Load data from CSV
csv_path = root_path + "datasets/competencia_01_crudo.csv"
df = pd.read_csv(csv_path)

# Create a table in DuckDB and insert the data
con.execute("CREATE OR REPLACE TABLE competencia_01 AS SELECT * FROM df")

#%% creacion target

query = """create or replace table competencia_01 as
with periodos as (
  select distinct foto_mes from competencia_01
), clientes as (
  select distinct numero_de_cliente from competencia_01
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
  left join competencia_01 c using (numero_de_cliente, foto_mes)
) select
* EXCLUDE (mes_0, mes_1, mes_2)
from clase_ternaria
where mes_0 = 1"""


con.execute(query)


#%% imputacion de nulos si corresponde



#%% creacion de lags

# con.execute("""select numero_de_cliente, foto_mes,
#             lag(foto_mes, 1) over (partition by numero_de_cliente order by foto_mes) as lag1_foto_mes,
#             lag(foto_mes, 2) over (partition by numero_de_cliente order by foto_mes) as lag2_foto_mes,
#              from competencia_01 limit 50""").fetchdf()

columnas = con.execute("""SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01';""").fetchdf()

columnas = columnas["column_name"]

columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

query = ", ".join([f"""LAG({col}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{col},
                   LAG({col}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{col}"""
                     for col in columnas])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_lags AS
            SELECT numero_de_cliente, foto_mes, clase_ternaria,
            {query}
            FROM competencia_01;""")

#%% calcular deltas -query

# con.execute("""select numero_de_cliente, foto_mes,
#               lag(foto_mes, 1) over (partition by numero_de_cliente order by foto_mes) as lag1_foto_mes,
#              lag(foto_mes, 2) over (partition by numero_de_cliente order by foto_mes) as lag2_foto_mes,
#             lag1_foto_mes,
#             lag2_foto_mes,
#             foto_mes/ lag1_foto_mes as delta1_foto_mes,
#             foto_mes/ lag2_foto_mes as delta2_foto_mes,
#             foto_mes/ - lag1_foto_mes as delta1_foto_mes,
#             - foto_mes/ lag2_foto_mes as delta2_foto_mes,
#              from competencia_01 limit 50""").fetchdf()



columnas = con.execute("""SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01';""").fetchdf()

columnas = columnas["column_name"]

columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

# exclude columns that start with 'lag1|lag2'
# Use regex to exclude columns that start with 'lag1' or 'lag2'
pattern = re.compile(r'^(lag1|lag2)')
columnas = [col for col in columnas if not pattern.match(col)]


query = ", ".join([f"""({col}/lag1_{col})-1 as variacion1_{col},
                   ({col}/lag2_{col})-1 as variacion2_{col}"""
                     for col in columnas])

#%% calcular deltas - execute
con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_deltas AS
            SELECT competencia_01.numero_de_cliente, competencia_01.foto_mes, competencia_01.clase_ternaria,
            {query}
            FROM competencia_01
            JOIN competencia_01_lags
            ON competencia_01.foto_mes = competencia_01_lags.foto_mes
            AND competencia_01.numero_de_cliente = competencia_01_lags.numero_de_cliente
            AND competencia_01.clase_ternaria = competencia_01_lags.clase_ternaria;""")


#%% columnas montos a decilado

columnas = con.execute("""SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01';""").fetchdf()

columnas = columnas["column_name"]

# get columns that start with 'm'
columnas_seleccionadas =  columnas[columnas.str.startswith("m")]

columnas_noseleccionadas = columnas[~columnas.str.startswith("m")]

columnas_noseleccionadas = ", ".join(columnas_noseleccionadas)

query = ", ".join([f"""ntile(200) over (partition by foto_mes order by {col}) as ntile_{col}"""
                     for col in columnas_seleccionadas])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01 AS
            SELECT {columnas_noseleccionadas},
            {query}
            FROM competencia_01;""")


#%% 

# Load rules from CSV (rankscsv.txt)
reglas = pd.read_csv(root_path+"aux-files/rankscsv.txt")

# Filter out rows where percentil, decil, cuartil are all NaN
reglas = reglas[['var', 'percentil', 'decil', 'cuartil']]
reglas = reglas.dropna(subset=['percentil', 'decil', 'cuartil'], how='all')

#macros
# Create or replace macros
con.execute("CREATE OR REPLACE MACRO safe_sum(a, b) AS ifnull(a, 0) + ifnull(b, 0);")
con.execute("CREATE OR REPLACE MACRO safe_minus(a, b) AS ifnull(a, 0) - ifnull(b, 0);")
con.execute("""
CREATE OR REPLACE MACRO safe_division(a, b) AS (
    CASE 
        WHEN a IS NULL AND b IS NOT NULL AND b != 0 THEN 0 
        WHEN a IS NULL AND b = 0 THEN 1 
        WHEN b IS NULL and a > 0 THEN 1.7976931348623157E+308
        WHEN b IS NULL and a < 0 THEN -1.7976931348623157E+308
        WHEN b = 0 and a > 0 THEN 1.7976931348623157E+308
        WHEN b = 0 and a < 0 THEN -1.7976931348623157E+308
        WHEN b IS NULL and a IS NULL THEN 1
        WHEN b IS NULL and a = 0 THEN 1
        WHEN b = 0 and a = 0 THEN 1
        ELSE a / b
    END
);""")

# Function to create SQL queries for percentiles, deciles, quartiles
def create_ranks(var, percentil, decil, cuartil):
    q1, q2, q3 = None, None, None
    if percentil:
        q1 = f"ntile(100) over (partition by foto_mes order by {var}) AS percentil_{var}"
    if decil:
        q2 = f"ntile(10) over (partition by foto_mes order by {var}) AS decil_{var}"
    if cuartil:
        q3 = f"ntile(4) over (partition by foto_mes order by {var}) AS cuartil_{var}"
    return ", ".join(filter(None, [q1, q2, q3]))


#%%

columnas = con.execute("""SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01_columnas_adicionales';""").fetchdf()

columnas = columnas["column_name"]

columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes|clase_ternaria")]

#%%

querys_lag = ", ".join([f"""LAG({col}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{col},
                        LAG({col}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{col}""" for col in columnas])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS  
    SELECT *, 
    {querys_lag}
    FROM competencia_01_columnas_adicionales
""")

#%%

# Create a list of queries based on rules
querys_ranks = [create_ranks(row['var'], row['percentil'], row['decil'], row['cuartil']) for _, row in reglas.iterrows()]
querys_ranks = ", ".join([q for q in querys_ranks if q])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
    SELECT *, {querys_ranks}
    FROM competencia_01_columnas_adicionales
""")

#%% creacion de lags

# creacion de lags --------------------------------------------------------


#%% variables ad hoc

# Run safe division query
# con.execute("""
#     CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
#     SELECT *, safe_division(mcuentas_saldo, lag(mcuentas_saldo, 1) over (partition by numero_de_cliente order by foto_mes)) - 1 as varx
#     FROM competencia_01
# """)

# Apply the queries for deciles, percentiles, quartiles to the table
# if querys_variaciones:
#     con.execute(f"""
#         CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
#         SELECT *, {querys_ranks}
#         FROM competencia_01_columnas_adicionales
#     """)

#%% calculo de variaciones

#%% 

# Exclude specific columns (like lag1_clase_ternaria)
excluded_column = "lag1_clase_ternaria"
con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
    SELECT * EXCLUDE ({excluded_column})
    FROM competencia_01_columnas_adicionales
""")

# Export the table to Parquet (can be CSV as well)
con.execute("COPY competencia_01_columnas_adicionales TO 'datasets/competencia_01_aum.parquet' (FORMAT PARQUET);")

# Disconnect from the database
con.close()
