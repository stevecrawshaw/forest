library(duckdb)
library(DBI)
library(tidyverse)


db_path <- "data/forest.duckdb"
con <- dbConnect(duckdb(), dbdir = db_path, read_only = TRUE)
dbExecute(con, "INSTALL spatial;")
dbExecute(con, "LOAD spatial;")
# 1. Percentage tree cover in the LEP region ----

lep_area_m2 <- dbGetQuery(
  con,
  "
  SELECT SUM(ST_Area(ST_GeomFromWKB(shape))) AS area_m2
  FROM lep_boundary_tbl
"
) |>
  pull(area_m2)

nfi_area_m2 <- dbGetQuery(
  con,
  "
  SELECT SUM(area_m2) AS area_m2 FROM nfi_clipped
"
) |>
  pull(area_m2)

tow_area_m2 <- dbGetQuery(
  con,
  "
  SELECT SUM(tow_area_calc_m2) AS area_m2 FROM tow_in_lep_tbl
"
) |>
  pull(area_m2)

tree_cover_pct <- (nfi_area_m2 + tow_area_m2) / lep_area_m2 * 100

cat(
  sprintf("LEP area: %.0f ha", lep_area_m2 / 10000),
  "\n",
  sprintf("NFI woodland: %.0f ha", nfi_area_m2 / 10000),
  "\n",
  sprintf("Trees outside woodland: %.0f ha", tow_area_m2 / 10000),
  "\n",
  sprintf("Combined tree cover: %.1f%%", tree_cover_pct),
  "\n"
)

# 2. NFI breakdown by CATEGORY and IFT_IOA ----

nfi_breakdown <- dbGetQuery(
  con,
  "
  SELECT
    CATEGORY,
    IFT_IOA,
    ROUND(SUM(area_m2), 2) AS area_m2,
    ROUND(SUM(area_m2) / 10000, 2) AS area_ha
  FROM nfi_clipped
  GROUP BY CATEGORY, IFT_IOA
  ORDER BY area_m2 DESC
"
) |>
  as_tibble()

nfi_breakdown

# 3. TOW breakdown by Woodland_Type ----

tow_breakdown <- dbGetQuery(
  con,
  "
  SELECT
    Woodland_Type,
    ROUND(SUM(tow_area_calc_m2), 2) AS area_m2,
    ROUND(SUM(tow_area_calc_m2) / 10000, 2) AS area_ha
  FROM tow_in_lep_tbl
  GROUP BY Woodland_Type
  ORDER BY area_m2 DESC
"
) |>
  as_tibble()

tow_breakdown

dbDisconnect(con, shutdown = TRUE)
