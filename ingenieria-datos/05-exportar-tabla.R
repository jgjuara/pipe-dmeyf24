library(DBI)
# Connect to my-db as defined in ~/.my.cnf
con <- dbConnect(
  RMySQL::MySQL(),
  user = Sys.getenv("usersrv"),
  password = Sys.getenv('password'),
  dbname = 'competencia_uno',
  host = Sys.getenv('ip'),
  port = 3306
)

tabla <- "datasets/competencia_01_aum.parquet"

df <- arrow::read_parquet(glue::glue("{tabla}"))

dbWriteTable(conn = con, value = df, name = "competencia_01_aum")
