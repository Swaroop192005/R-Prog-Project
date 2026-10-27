# High-Performance Big Data Analytics Using R – NYC Yellow Taxi Trips

**Laboratory Assignment 8 – R Programming** · Swaroop Naik

A scalable R analytics workflow for NYC Yellow Taxi trips. The committed run covers **9.2 million trips (January–March 2024)** on Google Colab; running locally analyses six months (~20 M trips). It covers
data preparation with **data.table**, transportation analytics, a comparison of **functional vs vectorized vs
data.table** programming, **sequential vs parallel** processing with `foreach` + `doParallel`, benchmarking with
`microbenchmark`, and visualizations with **ggplot2**, **sf** and **leaflet**.

![Architecture poster](poster/architecture_poster.png)

## Repository contents

| Path | Description |
|---|---|
| `NYC_Taxi_BigData_Analytics.ipynb` | Main notebook (R kernel), executed with all outputs |
| `NYC_Taxi_BigData_Analytics.pdf` | PDF export of the notebook (for submission) |
| `R/nyc_taxi_analysis.R` | Same code as a plain R script |
| `poster/architecture_poster.pdf` / `.png` / `.html` | A3 architecture poster |
| `outputs/figures/` | All figures (PNG) |
| `outputs/benchmark_results.csv` | Execution-time comparison table |
| `outputs/parallel_scaling.csv` | Parallel speedup per number of cores |
| `outputs/leaflet_revenue_by_zone.html` | Interactive leaflet map |

Notebook structure: Problem Statement → Dataset → Data Preparation (Task 1) → Analytics (Task 2) →
Functional/Vectorized/data.table (Task 3) → Parallel Computing (Task 4) → Benchmarking (Task 5) →
Visualizations (Task 6) → Observations → Critical Analysis (Task 7) → Conclusion.

## Dataset

* NYC TLC Yellow Taxi Trip Records, Jan–Jun 2024 – <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
* Taxi Zone Lookup Table and Taxi Zone shapefile (same page)

The raw data (about 340 MB of Parquet files) is **not stored in the repository**. The notebook downloads it into
`data/` the first time it runs.

## How to run

### Option A – Google Colab (nothing to install)

1. Download `NYC_Taxi_BigData_Analytics.ipynb` from this repository.
2. In Colab choose **File → Upload notebook**.
3. Choose **Runtime → Change runtime type → R**.
4. Choose **Runtime → Run all**, then **File → Print → Save as PDF** when it finishes.

The notebook detects Colab and adapts to it: it analyses **three months instead of six** (a Colab session has
about 12 GB of RAM, six months need about 11 GB at peak), installs the system libraries that `sf` needs, and
uses at least two worker cores because a Colab session has only two. It downloads the taxi data itself.

### Option B – locally with Jupyter

1. Install **R ≥ 4.2** and **Jupyter**, then register the R kernel:
   ```r
   install.packages("IRkernel"); IRkernel::installspec()
   ```
2. Install the R packages. The first notebook cell also installs any that are missing:
   ```r
   install.packages(c("data.table", "arrow", "ggplot2", "foreach", "doParallel", "microbenchmark",
                      "purrr", "lubridate", "scales", "hexbin", "leaflet", "htmlwidgets", "sf"))
   ```
3. Clone the repository and start Jupyter from the repository root:
   ```bash
   git clone https://github.com/Swaroop192005/R-Prog-Project.git
   cd R-Prog-Project
   jupyter notebook NYC_Taxi_BigData_Analytics.ipynb
   ```
   Then choose **Kernel → Restart & Run All**. The plain script runs the same steps:
   ```bash
   Rscript -e 'source("R/nyc_taxi_analysis.R", print.eval = TRUE)'
   ```
4. Export the PDF for submission: **File → Save and Export Notebook As → PDF / Webpdf**, or run
   `jupyter nbconvert --to webpdf NYC_Taxi_BigData_Analytics.ipynb`.

**Memory:** all six months need about 10–12 GB of RAM. On an 8 GB machine, or on Colab, set
`MONTHS <- sprintf("2024-%02d", 1:3)` in the setup cell (Colab does this automatically). A full run takes
about 18 minutes on 4 cores and about 25 minutes on Colab.

**Benchmark times and parallel speedups depend on the machine, so your numbers will differ.** The notebook is
written to handle that: section 6.7 derives its conclusion about parallel execution from the numbers actually
measured, so the text always matches the result. In the committed Colab run parallel execution gave **no
speedup** (3 partitions over 2 workers allow at most 1.5×, and worker start-up plus copying 421 MB cancels
it); on a 4-core machine with six monthly partitions the same code reaches about 2×.

## Key results (Google Colab, 2 vCPU, Jan–Mar 2024)

| Metric | Result |
|---|---|
| Trips analysed | 9,201,683 clean of 9,554,778 raw records (Jan–Mar 2024) |
| Peak demand | 18:00; busiest weekday Thursday, busiest month March |
| Revenue | airport trips ≈ 10 % of trips but ≈ 27 % of revenue; top route JFK Airport → Outside of NYC |
| Distance vs fare | correlation 0.942; fare per mile falls sharply as trips get longer |
| Payments | credit card 78.14 %, cash ≈ 13 % |
| Row-level: apply() vs vectorized | 2.1 s vs 0.5 ms (4461× slower) |
| Grouping 9.2M rows: aggregate() vs data.table | 8.6 s vs 334.6 ms (26×) |
| Join 2M rows: merge() vs data.table | 4.4 s vs 206.3 ms (21×, and 860 MB vs 153 MB peak) |
| Bootstrap: sequential vs parallel (2 cores) | 32.3 s vs 32.4 s — **no speedup** |
| Light task in parallel | 0.01× – about 100× slower than sequential, pure overhead |

## Recommendation

Use **data.table** as the core engine (import, clean, join and aggregate by reference) and write element-wise
logic in **vectorized** form. Use **functional programming** (`lapply` / `purrr::map`) to iterate over a few large
units such as files, columns or partitions. Add **foreach + doParallel** only for CPU-heavy, independent work,
and only after a benchmark shows that the speedup outweighs the start-up and data-transfer overhead.
