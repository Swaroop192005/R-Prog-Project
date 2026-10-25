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

# ---
# ## 4. Task 2 – Transportation Data Analytics
# All aggregations use data.table's `DT[i, j, by]` syntax.
#
# ### 4.1 Number of taxi trips by hour of day

hourly <- trips[, .(trips = .N, avg_fare = mean(fare_amount), avg_total = mean(total_amount)), by = pickup_hour][order(pickup_hour)]
hourly[, share_pct := round(100 * trips / sum(trips), 2)]
hourly
cat("Peak hour:", hourly[which.max(trips), pickup_hour], ":00 with", format(max(hourly$trips), big.mark = ","), "trips\n")
cat("Quietest hour:", hourly[which.min(trips), pickup_hour], ":00 with", format(min(hourly$trips), big.mark = ","), "trips\n")

# ### 4.2 Taxi demand across days of the week
# Because a six-month period does not contain the same number of Mondays, Tuesdays … , demand is also
# normalised as **average trips per day**.

dow <- trips[, .(trips = .N, days = uniqueN(pickup_date), avg_fare = round(mean(fare_amount), 2),
                 avg_distance = round(mean(trip_distance), 2)), by = pickup_wday][order(pickup_wday)]
dow[, avg_trips_per_day := round(trips / days)]
dow
dow_hour <- trips[, .N, by = .(is_weekend, pickup_hour)]
dow_hour[, days := ifelse(is_weekend, trips[is_weekend == TRUE, uniqueN(pickup_date)], trips[is_weekend == FALSE, uniqueN(pickup_date)])]
dow_hour[, trips_per_day := N / days]
dcast(dow_hour, pickup_hour ~ ifelse(is_weekend, "weekend", "weekday"), value.var = "trips_per_day")[, lapply(.SD, round)]

# ### 4.3 Monthly variation in taxi usage

monthly <- trips[, .(trips = .N, days = uniqueN(pickup_date), revenue_musd = round(sum(total_amount) / 1e6, 2),
                     avg_fare = round(mean(fare_amount), 2), avg_distance = round(mean(trip_distance), 2),
                     avg_duration_min = round(mean(trip_duration_min), 1)), by = month][order(month)]
monthly[, trips_per_day := round(trips / days)]
monthly[, change_vs_prev_pct := round(100 * (trips_per_day / shift(trips_per_day) - 1), 1)]
monthly

# ### 4.4 Average fare and total revenue for different time periods

period_stats <- trips[, .(trips = .N, avg_fare = round(mean(fare_amount), 2), avg_total = round(mean(total_amount), 2),
                          avg_distance = round(mean(trip_distance), 2), revenue_musd = round(sum(total_amount) / 1e6, 2)),
                      by = time_period][order(time_period)]
period_stats[, revenue_share_pct := round(100 * revenue_musd / sum(revenue_musd), 1)]
period_stats
trips[, .(trips = .N, avg_fare = round(mean(fare_amount), 2), avg_total = round(mean(total_amount), 2),
          revenue_musd = round(sum(total_amount) / 1e6, 2)), by = .(day_type = ifelse(is_weekend, "Weekend", "Weekday"))]

# ### 4.5 Most frequently travelled pickup – drop-off routes

routes <- trips[, .(trips = .N, avg_fare = mean(fare_amount), avg_distance = mean(trip_distance),
                    revenue = sum(total_amount)), by = .(pu_zone, do_zone)]
top_routes <- routes[order(-trips)][1:15]
top_routes[, `:=`(avg_fare = round(avg_fare, 2), avg_distance = round(avg_distance, 2), revenue_kusd = round(revenue / 1e3))][, revenue := NULL]
top_routes
cat("Distinct routes observed:", format(nrow(routes), big.mark = ","), "\n")
cat("Share of trips starting and ending in the same zone:",
    round(100 * trips[PULocationID == DOLocationID, .N] / nrow(trips), 2), "%\n")

# ### 4.6 Highest-revenue routes (total amount collected)

top_rev_routes <- routes[order(-revenue)][1:15]
top_rev_routes[, `:=`(revenue_musd = round(revenue / 1e6, 2), avg_fare = round(avg_fare, 2),
                      avg_distance = round(avg_distance, 1), revenue_per_trip = round(revenue / trips, 2))][, revenue := NULL]
top_rev_routes
airport_zones <- c("JFK Airport", "LaGuardia Airport", "Newark Airport")
trips[, .(trips = .N, revenue_musd = round(sum(total_amount) / 1e6, 1)),
      by = .(airport_trip = pu_zone %in% airport_zones | do_zone %in% airport_zones)][
      , `:=`(trip_share_pct = round(100 * trips / sum(trips), 1), revenue_share_pct = round(100 * revenue_musd / sum(revenue_musd), 1))][]

# ### 4.7 Relationship between trip distance and fare amount

cat("Pearson correlation (distance, fare)  :", round(trips[, cor(trip_distance, fare_amount)], 3), "\n")
samp_cor <- trips[sample(.N, 1e6)]
cat("Spearman correlation (1M-row sample)   :", round(samp_cor[, cor(trip_distance, fare_amount, method = "spearman")], 3), "\n")

fit <- lm(fare_amount ~ trip_distance, data = trips[RatecodeID == "Standard"])
cat(sprintf("Linear model (standard rate): fare = %.2f + %.2f x miles, R^2 = %.3f\n",
            coef(fit)[1], coef(fit)[2], summary(fit)$r.squared))

dist_bands <- trips[, .(trips = .N, avg_fare = round(mean(fare_amount), 2), median_fare = median(fare_amount),
                        fare_per_mile = round(mean(fare_amount / trip_distance), 2),
                        avg_duration_min = round(mean(trip_duration_min), 1)),
                    by = .(distance_band = cut(trip_distance, c(0, 1, 2, 3, 5, 10, 20, 100),
                                               labels = c("0-1", "1-2", "2-3", "3-5", "5-10", "10-20", "20+")))][order(distance_band)]
dist_bands

# ### 4.8 Payment methods across boroughs

pay_overall <- trips[, .(trips = .N, avg_total = round(mean(total_amount), 2)), by = payment_type][order(-trips)]
pay_overall[, share_pct := round(100 * trips / sum(trips), 2)]
pay_overall
pay_borough <- trips[!pu_borough %in% c("Unknown", "N/A"), .N, by = .(pu_borough, payment_type)]
pay_borough[, share_pct := round(100 * N / sum(N), 1), by = pu_borough]
dcast(pay_borough[payment_type %in% c("Credit card", "Cash", "Flex/Unknown")], pu_borough ~ payment_type, value.var = "share_pct", fill = 0)

# ### 4.9 Additional patterns

# (a) tipping behaviour by payment type (cash tips are not recorded by the meter)
trips[payment_type %in% c("Credit card", "Cash"),
      .(avg_tip = round(mean(tip_amount), 2), tip_pct_of_fare = round(100 * mean(tip_amount / fare_amount), 1),
        zero_tip_pct = round(100 * mean(tip_amount == 0), 1)), by = payment_type]

# (b) average speed by hour - congestion pattern
speed_hour <- trips[trip_duration_min >= 1, .(avg_speed_mph = round(mean(trip_distance / (trip_duration_min / 60)), 1)), by = pickup_hour][order(pickup_hour)]
cat("Slowest hour:", speed_hour[which.min(avg_speed_mph), pickup_hour], ":00 (", min(speed_hour$avg_speed_mph), "mph ) | fastest hour:",
    speed_hour[which.max(avg_speed_mph), pickup_hour], ":00 (", max(speed_hour$avg_speed_mph), "mph )\n")

# (c) passenger count distribution
trips[, .(trips = .N), by = .(passengers = pmin(passenger_count, 5L))][order(passengers)][, share_pct := round(100 * trips / sum(trips), 1)][]

# (d) days with the highest and lowest demand
daily <- trips[, .(trips = .N, revenue = sum(total_amount)), by = pickup_date][order(pickup_date)]
rbind(daily[order(-trips)][1:3][, type := "busiest"], daily[order(trips)][1:3][, type := "quietest"])

# **Interpretation of Task 2**
#
# * **Hourly demand** follows a clear commuter/leisure cycle: demand is lowest around 4–5 AM, builds through
#   the morning and peaks in the late afternoon / early evening (≈ 17–18 h). Average fares are highest in the
#   early morning because those trips are longer (airport and outer-borough runs).
# * **Day of week:** weekdays (especially Wed–Thu) have the highest average trips per day, Sunday the lowest.
#   The weekday/weekend hourly profile shows weekend demand shifted to late night (0–3 h).
# * **Monthly:** demand rises from January to spring (March–May are the busiest), reflecting winter
#   seasonality and tourism; June stays high.
# * **Revenue by period:** the midday and evening-rush periods generate most of the revenue, while early-morning
#   trips have the highest revenue per trip.
# * **Routes:** the most frequent routes are short trips inside Manhattan's Upper East Side; the
#   **highest-revenue routes all involve JFK / LaGuardia airports** – airport trips are a small share of trips
#   but a much larger share of revenue.
# * **Distance vs fare:** a strong positive, almost linear relationship (fare ≈ base + ≈ 4–5 USD per mile under the
#   standard rate) with high R², but the **fare per mile falls** sharply with distance because of the fixed
#   base charge and time-based charges on short congested trips.
# * **Payment:** credit cards dominate (≈ 80 % of trips); cash is relatively more common in the outer
#   boroughs. Recorded tips are almost exclusively on card payments.
# * **Other patterns:** taxis are slowest in the afternoon (congestion) and fastest early in the morning;
#   most trips carry a single passenger; holidays and storms produce the quietest days.

# ---
# ## 5. Task 3 – Functional vs Vectorized vs data.table Programming
#
# The same analytical operations are implemented in several styles and compared with `microbenchmark`
# (execution time) and `peak_mem_mb()` (peak extra memory). Row-wise functional code is far too slow for
# 20 million rows, so a **random sample of 200,000 trips** is used for the row-level experiment and
# **2 million trips** for the grouping experiment.

samp_dt <- trips[sample(.N, 2e5), .(fare_amount, trip_distance, tip_amount, total_amount, pu_borough, pickup_hour)]
samp_df <- as.data.frame(samp_dt)          # plain data.frame for the base-R versions
big_dt  <- trips[sample(.N, 2e6), .(fare_amount, trip_distance, tip_amount, total_amount, pu_borough, pickup_hour)]
big_df  <- as.data.frame(big_dt)
cat("Sample sizes:", nrow(samp_df), "and", nrow(big_df), "rows\n")
bench_results <- list()   # collects every benchmark for Task 5
add_bench <- function(mb, comparison, n_rows, mem = NULL) {
  s <- as.data.table(summary(mb, unit = "ms"))[, .(approach = as.character(expr), median_ms = round(median, 3), mean_ms = round(mean, 3))]
  s[, `:=`(comparison = comparison, rows = n_rows)]
  if (!is.null(mem)) s[, peak_mem_MB := mem[approach]]
  bench_results[[comparison]] <<- s
  s[, relative := round(median_ms / min(median_ms), 1)][order(median_ms)]
}

# ### 5.1 Experiment A – row-level calculation (fare per mile)
# Compute `fare_amount / trip_distance` for every trip.

fpm_apply  <- function(df) apply(df[, c("fare_amount", "trip_distance")], 1, function(r) r[1] / r[2])
fpm_lapply <- function(df) unlist(lapply(seq_len(nrow(df)), function(i) df$fare_amount[i] / df$trip_distance[i]))
fpm_map    <- function(df) purrr::map2_dbl(df$fare_amount, df$trip_distance, function(f, d) f / d)
fpm_loop   <- function(df) { out <- numeric(nrow(df)); for (i in seq_len(nrow(df))) out[i] <- df$fare_amount[i] / df$trip_distance[i]; out }
fpm_vector <- function(df) df$fare_amount / df$trip_distance
fpm_dt     <- function(dt) dt[, fare_amount / trip_distance]

# all implementations give the same answer
ref <- fpm_vector(samp_df)
stopifnot(all.equal(unname(fpm_apply(samp_df)), ref), all.equal(fpm_lapply(samp_df), ref),
          all.equal(fpm_map(samp_df), ref), all.equal(fpm_loop(samp_df), ref), all.equal(fpm_dt(samp_dt), ref))

mem_A <- c(`apply()` = peak_mem_mb(fpm_apply(samp_df)), `lapply()` = peak_mem_mb(fpm_lapply(samp_df)),
           `purrr::map2_dbl()` = peak_mem_mb(fpm_map(samp_df)), `for loop` = peak_mem_mb(fpm_loop(samp_df)),
           `vectorized` = peak_mem_mb(fpm_vector(samp_df)), `data.table` = peak_mem_mb(fpm_dt(samp_dt)))

mb_A <- microbenchmark(`apply()` = fpm_apply(samp_df), `lapply()` = fpm_lapply(samp_df),
                       `purrr::map2_dbl()` = fpm_map(samp_df), `for loop` = fpm_loop(samp_df),
                       `vectorized` = fpm_vector(samp_df), `data.table` = fpm_dt(samp_dt), times = 10)
add_bench(mb_A, "A: row-level fare per mile", nrow(samp_df), mem_A)

# ### 5.2 Experiment B – grouped aggregation (mean total amount per borough and hour)

grp_lapply <- function(df) {
  g <- split(df$total_amount, list(df$pu_borough, df$pickup_hour), drop = TRUE)
  unlist(lapply(g, mean))
}
grp_map    <- function(df) purrr::map_dbl(split(df$total_amount, list(df$pu_borough, df$pickup_hour), drop = TRUE), mean)
grp_sapply <- function(df) {           # conventional: filter the data once per group
  keys <- unique(df[, c("pu_borough", "pickup_hour")])
  sapply(seq_len(nrow(keys)), function(k) mean(df$total_amount[df$pu_borough == keys$pu_borough[k] & df$pickup_hour == keys$pickup_hour[k]]))
}
grp_tapply    <- function(df) tapply(df$total_amount, list(df$pu_borough, df$pickup_hour), mean)
grp_aggregate <- function(df) aggregate(total_amount ~ pu_borough + pickup_hour, data = df, FUN = mean)
grp_dt        <- function(dt) dt[, .(mean_total = mean(total_amount)), by = .(pu_borough, pickup_hour)]

# check: same group means from functional and data.table versions
chk <- grp_dt(big_dt)[order(pu_borough, pickup_hour)]
stopifnot(all.equal(unname(grp_lapply(big_df)[paste(chk$pu_borough, chk$pickup_hour, sep = ".")]), chk$mean_total))

mem_B <- c(`lapply(split())` = peak_mem_mb(grp_lapply(big_df)), `purrr::map_dbl(split())` = peak_mem_mb(grp_map(big_df)),
           `sapply() filter per group` = peak_mem_mb(grp_sapply(big_df)), `tapply()` = peak_mem_mb(grp_tapply(big_df)),
           `aggregate()` = peak_mem_mb(grp_aggregate(big_df)), `data.table by` = peak_mem_mb(grp_dt(big_dt)))

mb_B <- microbenchmark(`lapply(split())` = grp_lapply(big_df), `purrr::map_dbl(split())` = grp_map(big_df),
                       `sapply() filter per group` = grp_sapply(big_df), `tapply()` = grp_tapply(big_df),
                       `aggregate()` = grp_aggregate(big_df), `data.table by` = grp_dt(big_dt), times = 5)
add_bench(mb_B, "B: grouped mean by borough x hour", nrow(big_df), mem_B)

# ### 5.3 Experiment C – summary statistics over many columns
# Here the functional approach iterates over **columns** (only a handful of iterations), not rows.

num_vars <- c("fare_amount", "trip_distance", "tip_amount", "total_amount")
col_apply  <- function(df) apply(df[, num_vars], 2, mean)
col_lapply <- function(df) unlist(lapply(df[, num_vars], mean))
col_map    <- function(df) purrr::map_dbl(df[, num_vars], mean)
col_vector <- function(df) colMeans(df[, num_vars])
col_dt     <- function(dt) dt[, lapply(.SD, mean), .SDcols = num_vars]

stopifnot(all.equal(col_lapply(big_df), col_vector(big_df)), all.equal(unlist(col_dt(big_dt)), col_vector(big_df)))
mem_C <- c(`apply(, 2)` = peak_mem_mb(col_apply(big_df)), `lapply()` = peak_mem_mb(col_lapply(big_df)),
           `purrr::map_dbl()` = peak_mem_mb(col_map(big_df)), `colMeans() vectorized` = peak_mem_mb(col_vector(big_df)),
           `data.table .SD` = peak_mem_mb(col_dt(big_dt)))
mb_C <- microbenchmark(`apply(, 2)` = col_apply(big_df), `lapply()` = col_lapply(big_df), `purrr::map_dbl()` = col_map(big_df),
                       `colMeans() vectorized` = col_vector(big_df), `data.table .SD` = col_dt(big_dt), times = 10)
add_bench(mb_C, "C: column means (4 columns)", nrow(big_df), mem_C)

# ### 5.4 Comparison of code complexity and readability

data.table(
  approach      = c("apply()", "lapply() / sapply()", "purrr::map()", "for loop", "Vectorized base R", "data.table"),
  lines_of_code = c(1, 1, 1, 3, 1, 1),
  readability   = c("Medium - matrix conversion hidden", "Medium", "High - explicit, type-stable (map_dbl)",
                    "High for beginners, verbose", "Very high", "High once DT[i, j, by] is learnt"),
  complexity    = c("Low", "Low", "Low", "Medium (pre-allocation needed)", "Very low", "Low"),
  memory        = c("High - converts data.frame to matrix", "High - one R call + list element per row",
                    "High per row, low per column", "Low", "Low - one result vector", "Lowest - in-place, no copies"),
  large_data    = c("Unsuitable row-wise", "Unsuitable row-wise, fine over columns/groups",
                    "Unsuitable row-wise, fine over columns/groups", "Unsuitable", "Good", "Best (multi-threaded, by reference)")
)

# **Why the performance differs**
#
# * `apply()` first **coerces the data.frame to a matrix** (copying all values) and then calls an R function once
#   per row – 200,000 interpreted function calls. `lapply()`, `purrr::map()` and `for` loops avoid the matrix copy
#   but still pay the cost of one interpreted R call per element plus list allocation.
# * **Vectorized** arithmetic (`df$fare / df$dist`) performs a single call into compiled C code that loops over
#   the whole vector – typically **hundreds to thousands of times faster** than the row-wise versions.
# * For **grouping**, `split()` + `lapply()` is reasonable (the loop is over groups, not rows) but `split()`
#   copies the data into a list; the "filter per group" `sapply()` scans the full vector once per group;
#   `aggregate()` has heavy formula/model-frame overhead. **data.table** groups with a radix-sort based algorithm
#   in C, uses several threads (GForce optimisation for `mean()`), and allocates memory only for the result.
# * When the functional loop runs over **a few columns** (Experiment C), the call overhead is negligible:
#   `lapply()` / `map_dbl()` call the compiled `mean()` once per column and are as fast as data.table's `.SD`
#   and even faster than `colMeans()`, because `colMeans()` and `apply(, 2)` must first convert the
#   data.frame into a matrix (an extra copy). Functional style is the best choice there.

# ---
# ## 6. Task 4 – Parallel Computing with foreach + doParallel
#
# ### 6.1 Logical partitions
# The dataset is divided into **month-wise partitions** (6 subsets of ≈ 3 million trips). Only the columns needed
# by the analysis are kept so that less data has to be sent to the worker processes.

parts <- split(trips[, .(month, pu_borough, fare_amount, total_amount, trip_distance, trip_duration_min, tip_amount)],
               by = "month", keep.by = TRUE, drop = TRUE)
data.table(month = names(parts), rows = sapply(parts, nrow), size_MB = round(sapply(parts, function(p) object.size(p)) / 2^20, 1))

# ### 6.2 Computationally intensive operation – bootstrap analysis per month
# For every month we estimate **95 % bootstrap confidence intervals** for the mean total amount, the median trip
# distance and the mean fare per mile (`B` resamples of ≈ 3 million rows each), plus borough-level statistics.
# Resampling millions of rows many times is CPU-bound and each month is independent – a good candidate for
# parallelism.

analyse_partition <- function(d, B = 20) {
  data.table::setDTthreads(1)              # one thread per worker -> fair sequential vs parallel comparison
  set.seed(1000 + match(as.character(d$month[1]), month.abb))  # reproducible, identical in both modes
  n <- nrow(d)
  boot <- t(vapply(seq_len(B), function(b) {
    idx <- sample.int(n, n, replace = TRUE)
    c(mean_total    = mean(d$total_amount[idx]),
      median_dist   = median(d$trip_distance[idx]),
      fare_per_mile = mean(d$fare_amount[idx] / d$trip_distance[idx]))
  }, numeric(3)))
  ci <- apply(boot, 2, quantile, probs = c(0.025, 0.975))
  data.table::data.table(
    month = as.character(d$month[1]), trips = n,
    mean_total = mean(d$total_amount), mean_total_lo = ci[1, "mean_total"], mean_total_hi = ci[2, "mean_total"],
    median_dist = median(d$trip_distance), median_dist_lo = ci[1, "median_dist"], median_dist_hi = ci[2, "median_dist"],
    fare_per_mile = mean(d$fare_amount / d$trip_distance),
    manhattan_share = mean(d$pu_borough == "Manhattan"))
}
B <- 20
old_threads <- getDTthreads()

# ### 6.3 A. Sequential processing

t_seq <- system.time(res_seq <- rbindlist(lapply(parts, analyse_partition, B = B)))
t_seq_foreach <- system.time(res_seq_fe <- foreach(d = parts, .combine = rbind) %do% analyse_partition(d, B = B))
setDTthreads(old_threads)
cat(sprintf("Sequential lapply()      : %.2f s\nSequential foreach %%do%% : %.2f s\n", t_seq[["elapsed"]], t_seq_foreach[["elapsed"]]))
res_seq[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 3) else x)]

# ### 6.4 B. Parallel processing with foreach + doParallel
# A PSOCK cluster (works on Windows, macOS and Linux) is created with `N_CORES` workers; `foreach(... ) %dopar%`
# sends one monthly partition to each free worker and `rbind`s the results.

t_cluster <- system.time({
  cl <- makeCluster(N_CORES)
  registerDoParallel(cl)
  invisible(clusterEvalQ(cl, library(data.table)))
})
cat("Workers registered:", getDoParWorkers(), "| cluster start-up:", t_cluster[["elapsed"]], "s\n")

t_par <- system.time(
  res_par <- foreach(d = parts, .combine = rbind, .packages = "data.table") %dopar%
    analyse_partition(d, B = B)
)
cat(sprintf("Parallel foreach %%dopar%% with %d workers: %.2f s\n", N_CORES, t_par[["elapsed"]]))
stopifnot(all.equal(res_seq, res_par, check.attributes = FALSE))   # identical results
cat("Results identical to sequential run: TRUE\n")

# ### 6.5 Speedup and scaling with the number of cores
# $$\text{Speedup} = \frac{\text{Sequential execution time}}{\text{Parallel execution time}} \qquad \text{Efficiency} = \frac{\text{Speedup}}{\text{cores}}$$

stopCluster(cl)
core_grid <- unique(c(1L, 2L, seq(2L, N_CORES, by = 2L), N_CORES))
scaling <- rbindlist(lapply(core_grid, function(k) {
  cl_k <- makeCluster(k); registerDoParallel(cl_k)
  invisible(clusterEvalQ(cl_k, library(data.table)))
  el <- system.time(foreach(d = parts, .combine = rbind, .packages = "data.table", .export = "B") %dopar%
                      analyse_partition(d, B = B))[["elapsed"]]
  stopCluster(cl_k)
  data.table(cores = k, parallel_s = el)
}))
scaling[, `:=`(sequential_s = t_seq[["elapsed"]], speedup = round(t_seq[["elapsed"]] / parallel_s, 2))]
scaling[, efficiency_pct := round(100 * speedup / cores)]
scaling
speedup_main <- round(t_seq[["elapsed"]] / t_par[["elapsed"]], 2)
cat(sprintf("\nSpeedup with %d cores = %.2f s / %.2f s = %.2fx\n", N_CORES, t_seq[["elapsed"]], t_par[["elapsed"]], speedup_main))

# ### 6.6 When parallelism does not help – a lightweight task
# Computing a simple mean per month is so cheap that sending ≈ 600 MB of partitions to the workers costs far
# more than the computation itself.

light_task <- function(d) data.table::data.table(month = as.character(d$month[1]), mean_fare = mean(d$fare_amount))
cl <- makeCluster(N_CORES); registerDoParallel(cl)
t_light_seq <- system.time(foreach(d = parts, .combine = rbind) %do% light_task(d))[["elapsed"]]
t_light_par <- system.time(foreach(d = parts, .combine = rbind, .packages = "data.table") %dopar% light_task(d))[["elapsed"]]
t_light_dt  <- system.time(trips[, .(mean_fare = mean(fare_amount)), by = month])[["elapsed"]]
stopCluster(cl); registerDoSEQ()
light <- data.table(approach = c("Sequential foreach %do%", sprintf("Parallel foreach %%dopar%% (%d cores)", N_CORES), "data.table by = month (no partitions)"),
                    elapsed_s = c(t_light_seq, t_light_par, t_light_dt))
light[, speedup_vs_seq := round(t_light_seq / elapsed_s, 2)]
light

# **Discussion**
#
# * For the **CPU-intensive bootstrap**, the parallel version is clearly faster; the speedup grows with the
#   number of cores but stays **below the ideal linear speedup** (efficiency < 100 %) because of
#   (1) cluster start-up, (2) serialising each ≈ 100 MB partition to the worker processes,
#   (3) only 6 partitions for N workers (load imbalance: with 3 workers the last "round" is uneven when 6 is not
#   a multiple of the core count) and (4) memory-bandwidth contention.
# * For the **lightweight task** parallel execution is *slower* than sequential: the work per partition takes
#   milliseconds while copying the data to workers takes seconds. A single data.table grouping on the full table
#   (which is itself multi-threaded) is faster than both.
# * Therefore parallel processing pays off only when the computation per partition is large compared with the
#   communication overhead.

# ---
# ## 7. Task 5 – Performance Benchmarking and Optimization
#
# ### 7.1 Base R vs data.table on the full dataset
# Typical operations on all ≈ 20 million trips: filtering, grouped aggregation, sorting and joining.
# A base-R `data.frame` copy of the needed columns is used for the base-R versions.

full_df <- as.data.frame(trips[, .(PULocationID, pickup_hour, month, fare_amount, total_amount, trip_distance)])
full_dt <- as.data.table(full_df)
zones_df <- as.data.frame(zones)
n_full <- nrow(full_df)

mb_filter <- microbenchmark(
  `base R df[cond, ]`       = full_df[full_df$trip_distance > 10 & full_df$fare_amount > 50, ],
  `base R subset()`         = subset(full_df, trip_distance > 10 & fare_amount > 50),
  `data.table DT[cond]`     = full_dt[trip_distance > 10 & fare_amount > 50], times = 5)
add_bench(mb_filter, "D: filter long expensive trips", n_full)

mb_group <- microbenchmark(
  `base R aggregate()`      = aggregate(total_amount ~ month + pickup_hour, data = full_df, FUN = sum),
  `base R tapply()`         = tapply(full_df$total_amount, list(full_df$month, full_df$pickup_hour), sum),
  `data.table by`           = full_dt[, .(revenue = sum(total_amount)), by = .(month, pickup_hour)], times = 3)
add_bench(mb_group, "E: revenue by month x hour", n_full)

mb_sort <- microbenchmark(
  `base R order()`          = full_df[order(-full_df$total_amount), ],
  `data.table setorder()`   = setorder(copy(full_dt), -total_amount),
  `data.table DT[order()]`  = full_dt[order(-total_amount)], times = 3)
add_bench(mb_sort, "F: sort by total amount", n_full)

join_n  <- 2e6
join_df <- full_df[seq_len(join_n), ]
join_dt <- full_dt[seq_len(join_n)]
mem_join <- c(`base R merge()` = peak_mem_mb(merge(join_df, zones_df, by.x = "PULocationID", by.y = "LocationID", all.x = TRUE)),
              `base R match()` = peak_mem_mb({j <- join_df; j$Borough <- zones_df$Borough[match(j$PULocationID, zones_df$LocationID)]; j}),
              `data.table join` = peak_mem_mb(zones[join_dt, on = .(LocationID = PULocationID)]),
              `data.table update join :=` = peak_mem_mb(join_dt[zones, on = .(PULocationID = LocationID), Borough := i.Borough]))
mb_join <- microbenchmark(
  `base R merge()`          = merge(join_df, zones_df, by.x = "PULocationID", by.y = "LocationID", all.x = TRUE),
  `base R match()`          = {j <- join_df; j$Borough <- zones_df$Borough[match(j$PULocationID, zones_df$LocationID)]; j},
  `data.table join`         = zones[join_dt, on = .(LocationID = PULocationID)],
  `data.table update join :=` = join_dt[zones, on = .(PULocationID = LocationID), Borough := i.Borough], times = 3)
add_bench(mb_join, "G: join 2M trips with zones", join_n, mem_join)

# ### 7.2 Scalability – execution time vs number of rows
# The grouped aggregation (mean fare by pickup zone) is run on increasing subsets.

sizes <- c(1e5, 5e5, 1e6, 5e6, 1e7, n_full)
scal <- rbindlist(lapply(sizes, function(n) {
  df <- full_df[seq_len(n), c("PULocationID", "fare_amount")]
  dt <- full_dt[seq_len(n), .(PULocationID, fare_amount)]
  t <- summary(microbenchmark(
    `lapply(split())` = lapply(split(df$fare_amount, df$PULocationID), mean),
    `tapply()`        = tapply(df$fare_amount, df$PULocationID, mean),
    `aggregate()`     = aggregate(fare_amount ~ PULocationID, data = df, FUN = mean),
    `data.table by`   = dt[, mean(fare_amount), by = PULocationID], times = 3), unit = "ms")
  data.table(rows = n, approach = as.character(t$expr), median_ms = t$median)
}))
dcast(scal, rows ~ approach, value.var = "median_ms")[, lapply(.SD, round, 1)]

# ### 7.3 Consolidated execution-time comparison table

par_tbl <- data.table(approach = c("Sequential lapply()", "Sequential foreach %do%", sprintf("Parallel foreach %%dopar%% (%d cores)", N_CORES)),
                      median_ms = round(1000 * c(t_seq[["elapsed"]], t_seq_foreach[["elapsed"]], t_par[["elapsed"]]), 1),
                      comparison = "H: bootstrap per month (sequential vs parallel)", rows = nrow(trips))
bench_all <- rbindlist(c(bench_results, list(par_tbl)), fill = TRUE)
bench_all[, relative_to_best := round(median_ms / min(median_ms), 1), by = comparison]
bench_all <- bench_all[order(comparison, median_ms), .(comparison, rows, approach, median_ms, relative_to_best, peak_mem_MB)]
fwrite(bench_all, file.path(OUT_DIR, "benchmark_results.csv"))
fwrite(scaling, file.path(OUT_DIR, "parallel_scaling.csv"))
bench_all

# head-to-head summary for the four required comparisons
best_of <- function(cmp, pattern) bench_all[comparison == cmp & grepl(pattern, approach), min(median_ms)]
summary_tbl <- data.table(
  comparison = c("Base R vs data.table (grouping, 20M rows)", "Base R vs data.table (join, 2M rows)",
                 "Functional vs vectorized (row-level)", "Functional vs data.table (grouping)", "Sequential vs parallel (bootstrap)"),
  slower_approach_ms = c(best_of("E: revenue by month x hour", "aggregate"), best_of("G: join 2M trips with zones", "merge"),
                         best_of("A: row-level fare per mile", "lapply"), best_of("B: grouped mean by borough x hour", "lapply"),
                         1000 * t_seq[["elapsed"]]),
  faster_approach_ms = c(best_of("E: revenue by month x hour", "data.table"), best_of("G: join 2M trips with zones", "data.table"),
                         best_of("A: row-level fare per mile", "vectorized"), best_of("B: grouped mean by borough x hour", "data.table"),
                         1000 * t_par[["elapsed"]]))
summary_tbl[, speedup := round(slower_approach_ms / faster_approach_ms, 1)]
summary_tbl[, (2:3) := lapply(.SD, round, 1), .SDcols = 2:3]
summary_tbl

# **Analysis**
#
# * **Execution time:** data.table is the fastest approach for filtering, grouping and sorting, and its joins are
#   one to two orders of magnitude faster than `merge()`; grouped aggregation is far faster than `aggregate()`.
#   For a *single-column lookup* the vectorized base-R `match()` is even faster than a data.table join, because it
#   only builds one integer index – a good example that a well-chosen vectorized primitive is hard to beat,
#   whereas data.table's join is the general tool for multi-column / many-to-many joins. For row-level
#   arithmetic, vectorized base R and data.table are equivalent and both are thousands of times faster than
#   `apply()` / `lapply()` / `map()`.
# * **Memory:** `merge()` and `aggregate()` create several intermediate copies; data.table joins and especially the
#   **update join `:=`** add a column in place, so the peak extra memory is only the size of the new column.
#   `apply()` needs an extra matrix copy of the data.
# * **Scalability:** all approaches grow roughly linearly with the number of rows, but with very different slopes –
#   at 20 million rows `aggregate()` needs many seconds while `data.table` stays well below a second, so the
#   gap widens as the data grows.
# * **Programming complexity / readability:** vectorized code and data.table one-liners are both short and
#   readable. `purrr::map_*()` is the most explicit and type-safe functional style but is slow when used per row.
#   Parallel code adds the most complexity (cluster set-up, exporting functions/packages, seeds, cleanup)
#   and only pays off for heavy, independent computations.
