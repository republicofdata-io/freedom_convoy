library("DBI")
con <- dbConnect(duckdb::duckdb(), dbdir = "discursus_mvp.db", read_only = TRUE)

res <- dbGetQuery(con, "select * from entities.actors")
print(res)

dbDisconnect(con, shutdown=TRUE)
