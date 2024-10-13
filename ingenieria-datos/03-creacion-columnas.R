library(duckdb)
library(tidyverse)


con <- dbConnect(duckdb(dbdir = "duckdb/competencia_01.duckdb"))

source("ingenieria-datos/macros_sql.R")


# data <- duckdb_read_csv(conn = con, "competencia_01",
#                         files = "datasets/competencia_01_imp_nulos.csv")

reglas <- read_csv("aux-files/rankscsv.txt")

# para los arboles lo unico importante es el orden
# trabajar con ranks o deciles etc nos permite despreocuparnos de temas relacionados a cambios nominales de mes a mes
# traerse el t-1 y t-2 de todas las variables
# jugar con funciones de min,max ej max()


dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select * 
                from competencia_01"))


# limite credito compra total ----------------------------------------------------

query <- "Master_mlimitecompra + Visa_mlimitecompra as total_mlimitecompra"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query}
                from competencia_01_columnas_adicionales"))

# limite financiacion total -----------------------------------------------

# Visa_mfinanciacion_limite + Master_mfinanciacion_limite

query <- "Visa_mfinanciacion_limite + Master_mfinanciacion_limite as total_mfinanciacion_limite"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query},
                from competencia_01_columnas_adicionales"))


# saldo total credito -----------------------------------------------------

# Master_msaldototal + Visa_msaldototal

query <- "Master_msaldototal + Visa_msaldototal as total_msaldototal"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query},
                from competencia_01_columnas_adicionales"))


#  patrimonio en cuenta ---------------------------------------------------

# mcuentas_saldo + minversion1_pesos + minversion2 + mplazo_fijo_dolares +mplazo_fijo_pesos


query <- "mcuentas_saldo + minversion1_pesos + minversion2 + mplazo_fijo_dolares + mplazo_fijo_pesos as total_patrimonio"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query},
                from competencia_01_columnas_adicionales"))

# suma haberes ---------------------------------------------------

# cpayroll2_trx + cpayroll_trx 

query <- "cpayroll2_trx + cpayroll_trx as total_payroll"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query}
                from competencia_01_columnas_adicionales"))

# prestamos  ---------------------------------------------------

# mprestamos_personales + mprestamos_prendarios + mprestamos_hipotecarios

query <- "mprestamos_personales + mprestamos_prendarios + mprestamos_hipotecarios as total_prestamos"

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query}
                from competencia_01_columnas_adicionales"))

# creacion de ranks -------------------------------------------------------


columnas <- dbGetQuery(con, "SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01_columnas_adicionales';")


columnas <- columnas[["column_name"]]



reglas <- reglas %>% select(var, percentil, decil, cuartil)

reglas  <- reglas %>% filter(!if_all(c(percentil, cuartil, decil),
                                     is.na))

columnas <- columnas[! columnas %in% reglas$var]


querys_prep <- pmap(reglas, 
                    function(var, percentil, decil, cuartil) {
                      
                      q1 <- NULL
                      q2 <- NULL
                      q3 <- NULL
                      
                      if (isTRUE(percentil)) {
                        
                        q1 <- glue::glue("ntile(100) over (partition by foto_mes order by {var}) AS {var}")
                        
                      }
                      
                      # if (isTRUE(decil)) {
                      #   
                      #   q2 <- glue::glue("ntile(10) over (partition by foto_mes order by {var}) AS decil_{var}")
                      #   
                      # }
                      
                      # if (isTRUE(cuartil)) {
                      #   
                      #   q3 <- glue::glue("ntile(4) over (partition by foto_mes order by {var}) AS cuartil_{var}")
                      #   
                      # }
                      
                      # paste(c(q1,q2,q3), collapse = ", ")
                      
                      q1
                      
                    })


querys_prep <- querys_prep %>% unlist()

querys_prep <- paste(querys_prep, collapse = ", ")

columnas <- columnas %>% 
  paste(collapse = ", ")

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select {columnas}, 
                {querys_prep}
                from competencia_01_columnas_adicionales"))


# exportparquet_query <-  "COPY competencia_01 TO 'datasets/competencia_01_ranks.parquet' (FORMAT PARQUET);"
# exportcsv_query <-  "COPY competencia_01 TO 'datasets/competencia_01_ranks.csv' (HEADER, DELIMITER ',');"
# 
# dbExecute(con, exportparquet_query)
# dbExecute(con, exportcsv_query)


# ranks vars ad hoc -----

vars_adhoc <- c("total_mlimitecompra", "total_mfinanciacion_limite",
                "total_msaldototal", "total_patrimonio",  "total_payroll", "total_prestamos")

query <- sapply(vars_adhoc, function(var) {glue::glue("ntile(100) over (partition by foto_mes order by {var}) AS {var}")})

query <- paste(query, collapse = ", ")


dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {query}
                from competencia_01_columnas_adicionales"))

# creacion de lags --------------------------------------------------------

columnas <- dbGetQuery(con, "SELECT column_name
FROM information_schema.columns
WHERE table_name = 'competencia_01_columnas_adicionales';")


columnas <- columnas[["column_name"]]

columnas <- columnas[! grepl("numero_de_cliente|foto_mes", columnas)]


querys_lag <- sapply(columnas, function(x) {
  glue::glue("LAG({x}, 1) over (partition by numero_de_cliente order by foto_mes) AS lag1_{x},
             LAG({x}, 2) over (partition by numero_de_cliente order by foto_mes) AS lag2_{x}")
})

querys_lag <- paste(querys_lag, collapse = ", ")

dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {querys_lag},
                from competencia_01_columnas_adicionales"))

# exportparquet_query <-  "COPY competencia_01 TO 'datasets/competencia_01_ranks.parquet' (FORMAT PARQUET);"
# exportcsv_query <-  "COPY competencia_01 TO 'datasets/competencia_01_lags.csv' (HEADER, DELIMITER ',');"
# 
# dbExecute(con, exportparquet_query)
# dbExecute(con, exportcsv_query)


# vars falopa -------------------------------------------------------------

# var si cumplio anios como var entre edad y lag(edad)
# 
# query <- "cliente_edad - lag1_cliente_edad as cumpleanios"
# 
# dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
#                 select *, 
#                 {query}
#                 from competencia_01_columnas_adicionales"))


# var cliente_antiguedad binned (responde a quienes entraron juntos)


# variaciones -------------------------------------------------------------

columnas <- dbGetQuery(con, "SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'competencia_01_columnas_adicionales'
      AND data_type IN ('INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'REAL', 'NUMERIC', 'DECIMAL');")

columnas <- columnas[["column_name"]]

columnas <- columnas[! grepl("numero_de_cliente|foto_mes", columnas)]

columnas <- columnas[! grepl("lag1|cumpleanios", columnas)]


# querys_reg <- sapply(columnas, function(x) {
#   glue::glue("regr_slope(foto_mes, {x}) over ventana_3 as betareg_{x}")
# })
querys_variaciones <- sapply(columnas,
                             function(x) {
                               glue::glue("safe_division({x}, lag({x}, 1) over (partition by numero_de_cliente order by foto_mes)) - 1 AS variacion1_{x},
                                          safe_division(lag({x}, 1) over (partition by numero_de_cliente order by foto_mes), lag({x}, 2) over (partition by numero_de_cliente order by foto_mes)) - 1 AS variacion2_{x}")
                               })

# querys_reg <- paste(querys_reg, collapse = ", ")

querys_variaciones <- paste(querys_variaciones, collapse = ", ")

# dbGetQuery(con, glue::glue("select safe_division(mcuentas_saldo, lag(mcuentas_saldo, 1) over (partition by numero_de_cliente order by foto_mes)) - 1 as varx
#                 from competencia_01_columnas_adicionales"))

  dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select *, 
                {querys_variaciones}
                from competencia_01_columnas_adicionales"))
                # window ventana_3 as (partition by numero_de_cliente order by foto_mes rows between 1 preceding and current row)"))





# exlcusion de columnas ---------------------------------------------------

  
# columnas <- dbGetQuery(con, "SELECT column_name
# FROM information_schema.columns
# WHERE table_name = 'competencia_01';")
# 
# columnas <- columnas[["column_name"]]
  
columnas <- "lag1_clase_ternaria"
  
  
dbExecute(con, glue::glue("create or replace table competencia_01_columnas_adicionales as  
                select * exclude ({columnas})
                from competencia_01_columnas_adicionales;"))
  
  
# exportcsv_query <-  "COPY competencia_01_columnas_adicionales TO 'datasets/competencia_01_aum.csv' (HEADER, DELIMITER ',');"
# 
exportparquet_query <-  "COPY competencia_01_columnas_adicionales TO 'datasets/competencia_01_aum.parquet' (FORMAT PARQUET);"
dbExecute(con, exportparquet_query)
# dbExecute(con, exportcsv_query)

dbDisconnect(con)

# columnas <- dbGetQuery(con, "SELECT column_name
# FROM information_schema.columns
# WHERE table_name = 'competencia_01_columnas_adicionales';")
# 
# x <- list()
# for (i in columnas$column_name[!grepl("foto_mes",columnas$column_name )]) {
#   x[[i]] <- dbGetQuery(con, glue::glue("select {i}, foto_mes
#                                    from competencia_01_columnas_adicionales
#                                   where {i} is NULL;"))
# 
# }

# x %>% bind_rows(, .id = "var") %>% 
#   pivot_longer(cols = everything()) %>% view
