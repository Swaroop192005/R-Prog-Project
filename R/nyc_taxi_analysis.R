# NYC Yellow Taxi - High-Performance Big Data Analytics in R
# Script version of NYC_Taxi_BigData_Analytics.ipynb (run from the repository root:
#   Rscript -e 'source("R/nyc_taxi_analysis.R", print.eval = TRUE)' )

# # High-Performance Big Data Analytics Using R
# ## NYC Yellow Taxi Trip Records (January – June 2024)
#
# **Laboratory Assignment 8 – R Programming**
#
# **Author:** Swaroop Naik ([@Swaroop192005](https://github.com/Swaroop192005))
#
# ---
#
# ### Contents
# 1. Problem Statement
# 2. Dataset
# 3. Task 1 – Data Acquisition and Preparation
# 4. Task 2 – Transportation Data Analytics
# 5. Task 3 – Functional vs Vectorized vs data.table Programming
# 6. Task 4 – Parallel Computing (foreach + doParallel)
# 7. Task 5 – Performance Benchmarking and Optimization
# 8. Task 6 – Data Visualization (ggplot2 + leaflet)
# 9. Observations
# 10. Task 7 – Critical Analysis and Recommendation
# 11. Conclusion

# ## 1. Problem Statement
#
# The New York City Taxi and Limousine Commission (NYC TLC) publishes millions of taxi-trip records every month.
# Each record contains pickup/drop-off timestamps and locations, passenger count, trip distance, fare, tip,
# tolls, payment type and the total amount charged. Processing data at this scale with conventional
# row-by-row R code is slow and memory hungry.
#
# **Objective:** build a high-performance and scalable analytics workflow in R that
#
# * prepares and joins ~20 million trip records with the taxi-zone lookup table using **data.table**,
# * identifies travel patterns, high-demand periods, high-revenue routes, fare/distance characteristics and
#   payment preferences,
# * compares **functional programming** (`apply()`, `lapply()`, `purrr::map()`) with **vectorized** and
#   **data.table** implementations,
# * compares **sequential** and **parallel** execution (`foreach` + `doParallel`) on month-wise partitions,
# * benchmarks every approach with `microbenchmark` / `system.time()` and memory measurements, and
# * critically evaluates the approaches for execution time, memory, scalability and readability.

# ## 2. Dataset
#
# | Item | Description |
# |---|---|
# | Source | NYC TLC Trip Record Data – <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> |
# | Trip files | Yellow Taxi Trip Records, **January – June 2024** (6 monthly Parquet files, ≈ 20 million trips) |
# | Lookup table | Taxi Zone Lookup Table (`taxi_zone_lookup.csv`, 265 zones) + Taxi Zone shapefile (for the map) |
#
# Main attributes used:
#
# | Column | Meaning |
# |---|---|
# | `tpep_pickup_datetime`, `tpep_dropoff_datetime` | Pickup / drop-off date and time |
# | `PULocationID`, `DOLocationID` | TLC taxi-zone IDs of pickup / drop-off |
# | `passenger_count` | Number of passengers |
# | `trip_distance` | Trip distance in miles |
# | `payment_type` | 1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, 0 = Flex fare / unknown |
# | `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount` | Monetary amounts in USD |
#
# Six months were chosen because they give a sufficiently large dataset (≈ 3 million rows per month) so that
# the performance differences between conventional, optimized, sequential and parallel implementations are
# clearly visible, while still fitting in the memory of a normal laptop. Change `MONTHS` in the next cell to
# use fewer months on a machine with less than 8 GB RAM.

# ### 2.1 Environment setup
# Install (if needed) and load the packages. `TZ = "UTC"` is set so that the timestamps, which the TLC stores
# as New York local time without a time zone, are kept exactly as recorded.

required <- c("data.table", "arrow", "ggplot2", "foreach", "doParallel", "microbenchmark",
              "purrr", "lubridate", "scales", "hexbin", "leaflet", "htmlwidgets", "sf")
missing_pkgs <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing_pkgs) > 0) install.packages(missing_pkgs, repos = "https://cloud.r-project.org")

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(ggplot2)
  library(foreach)
  library(doParallel)
  library(microbenchmark)
  library(purrr)
  library(lubridate)
  library(scales)
})

Sys.setenv(TZ = "UTC")
set.seed(2024)
options(repr.plot.width = 12, repr.plot.height = 6, scipen = 999, width = 140)

MONTHS   <- sprintf("2024-%02d", 1:6)        # analysis period: Jan - Jun 2024
DATA_DIR <- "data"
OUT_DIR  <- "outputs"
FIG_DIR  <- file.path(OUT_DIR, "figures")
for (d in c(DATA_DIR, FIG_DIR)) dir.create(d, showWarnings = FALSE, recursive = TRUE)

N_CORES <- max(1L, parallel::detectCores() - 1L)   # keep one core free for the OS

cat("R version          :", R.version.string, "\n")
cat("data.table version :", as.character(packageVersion("data.table")), "\n")
cat("Logical cores      :", parallel::detectCores(), "| cores used for parallel work:", N_CORES, "\n")
cat("data.table threads :", getDTthreads(), "\n")

# ### 2.2 Helper functions
# * `peak_mem_mb()` – peak extra memory (MB) used by R while evaluating an expression, measured with `gc()`.
# * `save_plot()` – saves a ggplot to `outputs/figures/` and returns it so it is also shown in the notebook.

peak_mem_mb <- function(expr) {
  invisible(gc(reset = TRUE))
  before <- sum(gc()[, 2])                  # MB in use now
  force(expr)
  peak <- sum(gc()[, 6])                    # max MB used since reset
  round(peak - before, 1)
}

save_plot <- function(p, name, width = 11, height = 6) {
  ggsave(file.path(FIG_DIR, paste0(name, ".png")), p, width = width, height = height, dpi = 120, bg = "white")
  p
}

theme_set(theme_minimal(base_size = 13) +
          theme(plot.title = element_text(face = "bold"), legend.position = "bottom"))

# ### 2.3 Download the data
# Files are downloaded once from the official TLC CloudFront location and re-used afterwards
# (the `data/` folder is not committed to Git).

options(timeout = 3600)
base_url   <- "https://d37ci6vzurychx.cloudfront.net"
trip_files <- file.path(DATA_DIR, sprintf("yellow_tripdata_%s.parquet", MONTHS))

for (i in seq_along(MONTHS)) {
  if (!file.exists(trip_files[i])) {
    download.file(sprintf("%s/trip-data/yellow_tripdata_%s.parquet", base_url, MONTHS[i]),
                  trip_files[i], mode = "wb")
  }
}
zone_file <- file.path(DATA_DIR, "taxi_zone_lookup.csv")
if (!file.exists(zone_file)) download.file(paste0(base_url, "/misc/taxi_zone_lookup.csv"), zone_file, mode = "wb")
shape_zip <- file.path(DATA_DIR, "taxi_zones.zip")
if (!file.exists(shape_zip)) download.file(paste0(base_url, "/misc/taxi_zones.zip"), shape_zip, mode = "wb")

data.table(file = basename(c(trip_files, zone_file, shape_zip)),
           size_MB = round(file.size(c(trip_files, zone_file, shape_zip)) / 2^20, 1))
