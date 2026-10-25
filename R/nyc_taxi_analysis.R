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

# ---
# ## 3. Task 1 – Data Acquisition and Preparation
#
# ### 3.1 Import the trip records
# `arrow::open_dataset()` treats the six Parquet files as one dataset; `as.data.frame()` reads it in a single pass
# and `setDT()` converts the result to a `data.table` **by reference** (no extra copy).

t_import <- system.time({
  trips <- setDT(as.data.frame(open_dataset(trip_files, unify_schemas = TRUE)))
})
cat(sprintf("Imported %s rows x %d columns in %.1f s\n",
            format(nrow(trips), big.mark = ","), ncol(trips), t_import[["elapsed"]]))
raw_size <- object.size(trips)
cat("Memory used by raw table:", format(raw_size, units = "auto"), "\n")

# ### 3.2 Inspect structure, dimensions, data types and summary statistics

dim(trips)
str(trips)

num_cols <- names(trips)[vapply(trips, is.numeric, logical(1))]
stats <- rbindlist(lapply(num_cols, function(cn) {
  x <- trips[[cn]]
  data.table(column = cn, min = min(x, na.rm = TRUE), mean = mean(x, na.rm = TRUE),
             median = median(x, na.rm = TRUE), max = max(x, na.rm = TRUE), missing = sum(is.na(x)))
}))
stats[, (2:5) := lapply(.SD, round, 2), .SDcols = 2:5]
stats
cat("Pickup time range:", format(range(trips$tpep_pickup_datetime)), "\n")

# ### 3.3 Missing, duplicate, inconsistent and invalid records
# First we count missing values per column.

na_counts <- trips[, lapply(.SD, function(x) sum(is.na(x)))]
na_counts <- melt(na_counts, measure.vars = names(na_counts), variable.name = "column", value.name = "missing")
na_counts[, pct := round(100 * missing / nrow(trips), 2)]
na_counts[missing > 0]
# Rows with missing passenger_count also miss RatecodeID / store_and_fwd_flag and have payment_type 0
trips[is.na(passenger_count), .N, by = payment_type]

# **Cleaning rules** (each step is logged):
#
# | Problem | Rule | Action |
# |---|---|---|
# | Duplicates | identical vendor, timestamps, locations, distance and amount | remove |
# | Out-of-period records | pickup outside 1 Jan – 30 Jun 2024 (wrong meter clocks) | remove |
# | Missing `passenger_count` / `RatecodeID` | ≈ 10 % of rows (flex-fare / app trips) | **impute** passenger count with the median (1), RatecodeID with 99 = unknown |
# | Zero passengers | inconsistent with a paid trip | impute with median (1) |
# | Non-positive or extreme amounts | fare ≤ 0 or > 500 USD, total ≤ 0, negative tip/tolls | remove (refunds, voids, typos) |
# | Invalid distance | distance ≤ 0 or > 100 miles | remove |
# | Invalid duration | drop-off not after pickup or trip longer than 3 hours | remove |
# | Impossible speed | average speed > 80 mph | remove |
# | Invalid zone IDs | outside 1–265 | remove |

clean_log <- data.table(step = "Raw records", rows = nrow(trips))
log_step  <- function(step) clean_log <<- rbind(clean_log, data.table(step = step, rows = nrow(trips)))

# 1. exact duplicates (on the identifying columns)
trips <- unique(trips, by = c("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                              "PULocationID", "DOLocationID", "trip_distance", "total_amount"))
log_step("Remove duplicate trips")

# 2. pickups outside the analysis period
period_start <- as.POSIXct("2024-01-01 00:00:00", tz = "UTC")
period_end   <- as.POSIXct("2024-07-01 00:00:00", tz = "UTC")
trips <- trips[tpep_pickup_datetime >= period_start & tpep_pickup_datetime < period_end]
log_step("Remove pickups outside Jan-Jun 2024")

# 3. impute missing / zero passenger count and missing rate code (update by reference)
n_pass_fixed <- trips[is.na(passenger_count) | passenger_count == 0, .N]
trips[is.na(passenger_count) | passenger_count == 0, passenger_count := 1L]
trips[is.na(RatecodeID), RatecodeID := 99L]
cat("Passenger count imputed for", format(n_pass_fixed, big.mark = ","), "rows\n")

# 4. invalid amounts
trips <- trips[fare_amount > 0 & fare_amount <= 500 & total_amount > 0 & tip_amount >= 0 & tolls_amount >= 0]
log_step("Remove invalid fare / total / tip / tolls")

# 5. invalid distance
trips <- trips[trip_distance > 0 & trip_distance <= 100]
log_step("Remove invalid trip distance")

# 6. invalid duration (computed by reference, vectorized)
trips[, trip_duration_min := as.numeric(tpep_dropoff_datetime - tpep_pickup_datetime, units = "mins")]
trips <- trips[trip_duration_min > 0 & trip_duration_min <= 180]
log_step("Remove invalid trip duration")

# 7. impossible speed
trips <- trips[trip_distance / (trip_duration_min / 60) <= 80]
log_step("Remove speed > 80 mph")

# 8. invalid zone IDs
trips <- trips[PULocationID %between% c(1L, 265L) & DOLocationID %between% c(1L, 265L)]
log_step("Remove invalid zone IDs")

clean_log[, removed := shift(rows, fill = rows[1]) - rows]
clean_log[, pct_of_raw := round(100 * rows / rows[1], 2)]
clean_log

# ### 3.4 Convert attributes into appropriate data types
# Integer codes become labelled **factors** (stored as integers + labels – memory efficient and readable).

payment_labels <- c("0" = "Flex/Unknown", "1" = "Credit card", "2" = "Cash",
                    "3" = "No charge", "4" = "Dispute", "5" = "Unknown", "6" = "Voided")
rate_labels <- c("1" = "Standard", "2" = "JFK", "3" = "Newark", "4" = "Nassau/Westchester",
                 "5" = "Negotiated", "6" = "Group ride", "99" = "Unknown")

trips[, payment_type := factor(payment_labels[as.character(payment_type)],
                               levels = c("Credit card", "Cash", "Flex/Unknown", "Dispute", "No charge", "Unknown", "Voided"))]
trips[, RatecodeID := factor(rate_labels[as.character(RatecodeID)], levels = unique(rate_labels))]
trips[, passenger_count := as.integer(passenger_count)]
trips[, `:=`(PULocationID = as.integer(PULocationID), DOLocationID = as.integer(DOLocationID))]
sapply(trips[, .(payment_type, RatecodeID, passenger_count, PULocationID, trip_distance)], class)

# ### 3.5 Extract temporal attributes
# Hour, day of month, day of week (Monday first), month, date and a weekend flag. data.table's
# `hour()`, `mday()`, `wday()` and `month()` are fast vectorized C implementations.

wday_labels <- c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
t_time <- system.time({
  trips[, `:=`(
    pickup_date  = as.IDate(tpep_pickup_datetime),
    pickup_hour  = hour(tpep_pickup_datetime),
    pickup_day   = mday(tpep_pickup_datetime),
    pickup_wday  = factor((wday(tpep_pickup_datetime) + 5L) %% 7L + 1L, levels = 1:7, labels = wday_labels),
    month        = factor(month(tpep_pickup_datetime), levels = 1:6, labels = month.abb[1:6])
  )]
  trips[, is_weekend := pickup_wday %in% c("Sat", "Sun")]
  trips[, time_period := cut(pickup_hour, breaks = c(-1, 5, 9, 15, 19, 23),
                             labels = c("Early morning (0-5)", "Morning rush (6-9)", "Midday (10-15)",
                                        "Evening rush (16-19)", "Night (20-23)"))]
})
cat("Temporal features created in", t_time[["elapsed"]], "s\n")
head(trips[, .(tpep_pickup_datetime, pickup_date, pickup_hour, pickup_day, pickup_wday, month, is_weekend, time_period)])

# ### 3.6 Remove unnecessary attributes to reduce memory
# Surcharges and technical flags are not needed for the analysis. Columns are dropped **by reference**
# (`:= NULL`), which does not copy the table.

size_before <- object.size(trips)
drop_cols <- intersect(c("VendorID", "store_and_fwd_flag", "extra", "mta_tax", "improvement_surcharge",
                         "congestion_surcharge", "Airport_fee", "airport_fee"), names(trips))
trips[, (drop_cols) := NULL]
size_after <- object.size(trips)
invisible(gc())
data.table(stage = c("Raw import", "After cleaning + new features", "After dropping columns"),
           columns = c(19, ncol(trips) + length(drop_cols), ncol(trips)),
           size = c(format(raw_size, units = "MB"), format(size_before, units = "MB"), format(size_after, units = "MB")))

# ### 3.7 Import the Taxi Zone Lookup Table

zones <- fread(zone_file)
setkey(zones, LocationID)
cat("Zones:", nrow(zones), "\n")
zones[, .N, by = Borough][order(-N)]
head(zones)

# ### 3.8 Join trips with zone information
# An **update join** (`X[Y, on = , := ]`) adds pickup and drop-off borough/zone to the 20 million rows
# by reference – no copy of the large table is created (a `merge()` would copy it twice).

t_join <- system.time({
  trips[zones, on = .(PULocationID = LocationID), `:=`(pu_borough = i.Borough, pu_zone = i.Zone)]
  trips[zones, on = .(DOLocationID = LocationID), `:=`(do_borough = i.Borough, do_zone = i.Zone)]
  # repeated strings -> factors to save memory
  for (col in c("pu_borough", "pu_zone", "do_borough", "do_zone")) set(trips, j = col, value = factor(trips[[col]]))
})
cat("Two update joins on", format(nrow(trips), big.mark = ","), "rows took", t_join[["elapsed"]], "s\n")
setkey(trips, tpep_pickup_datetime)
trips[, .(trips = .N), by = pu_borough][order(-trips)]

# ### 3.9 Final prepared dataset

str(trips)
cat(sprintf("\nFinal dataset: %s trips (%.1f %% of raw records kept), %s in memory\n",
            format(nrow(trips), big.mark = ","), 100 * nrow(trips) / clean_log$rows[1],
            format(object.size(trips), units = "auto")))

# **Interpretation:** after removing duplicates, out-of-period, invalid and implausible records about
# 95 % of the raw records remain, so the cleaning removes noise without biasing the dataset. Converting codes to
# factors and dropping unused columns reduces the memory footprint, and every expensive step (feature creation,
# imputation, joins) was done **by reference** with data.table, which avoids copying a multi-gigabyte table.
