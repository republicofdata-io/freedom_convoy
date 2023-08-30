library("DBI")
con <- dbConnect(duckdb::duckdb(), dbdir = "social_analytics_mvp.db", read_only = TRUE)

res <- dbGetQuery(con, "select * from entities.actors")
print(res)

dbDisconnect(con, shutdown=TRUE)
