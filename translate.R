### 
# A SIMPLE SCRIPT TO TRANSLATE QUERIES
###

library(tidyverse)
library(SqlRender)

x <- "postgresql"

dir.create(x)
dir.create(paste0(x, "/BC"))
dir.create(paste0(x, "/NSCLC"))

for (i in list.files(path = "sql_server/", pattern = "*.sql", full.names = TRUE, recursive = TRUE)){


    sql <- SqlRender::readSql(i)
    sql <- SqlRender::translate(sql, targetDialect = x)
    SqlRender::writeSql(sql, str_replace(i, "sql_server", x))
    
}
