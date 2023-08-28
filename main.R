library("DBI")
con <- dbConnect(duckdb::duckdb(), dbdir = "discursus_mvp.db", read_only = TRUE)

res <- dbGetQuery(con, "show tables")
print(res)
