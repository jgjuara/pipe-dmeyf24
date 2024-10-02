#%%
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

root_path = Path(__file__).parent.parent.resolve()

root_path = root_path.as_posix() + '/'

# Connect to DuckDB database
con = duckdb.connect(root_path+'duckdb/competencia_01.duckdb')




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

# Create a list of queries based on rules
querys_ranks = [create_ranks(row['var'], row['percentil'], row['decil'], row['cuartil']) for _, row in reglas.iterrows()]
querys_ranks = ", ".join([q for q in querys_ranks if q])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
    SELECT *, {querys_ranks}
    FROM competencia_01
""")

#%% creacion de lags

# creacion de lags --------------------------------------------------------

columnas = con.execute("""SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01_columnas_adicionales';""").fetchdf()

columnas = columnas["column_name"]

columnas = columnas[~columnas.str.contains("numero_de_cliente|foto_mes")]

querys_lag = ", ".join([f"""LAG({col}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{col},
                        LAG({col}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{col}""" for col in columnas])

con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS  
    SELECT *, 
    {querys_lag}
    FROM competencia_01_columnas_adicionales
""")

#%% variables ad hoc

# Run safe division query
con.execute("""
    CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
    SELECT *, safe_division(mcuentas_saldo, lag(mcuentas_saldo, 1) over (partition by numero_de_cliente order by foto_mes)) - 1 as varx
    FROM competencia_01
""")

# Apply the queries for deciles, percentiles, quartiles to the table
if querys_variaciones:
    con.execute(f"""
        CREATE OR REPLACE TABLE competencia_01_columnas_adicionales AS
        SELECT *, {querys_ranks}
        FROM competencia_01_columnas_adicionales
    """)

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
