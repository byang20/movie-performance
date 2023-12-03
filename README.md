# Movie Success Analysis
NYU Processing Big Data for Analytics Applications Project

## Data
The data used for this project can be found in the following links.
1. https://data.world/mahe432/movies
2. https://data.world/hdx/3c7aca84-e00b-46a0-a8d5-d769b16e5351

The input data can be found in HDFS in the following folders respectively:
1. `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/input`
2. `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/input`

## How To Run
*This assumes that you are affiliated with NYU and are able to access the university's High Performance Cluster (HPC).*

### Movies
#### ETL
1. Run the /etl_code/brian/nullfill/nullfill.scala: `spark-shell --deploy-mode client -i nullfill.scala`
   * Pulls original data set from `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/input/IMDb-movies.csv`
   * Replaces blank fields with String "Null" and saves result to `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill`
2. Run `hdfs dfs -ls hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill` to find results, and take note of path of the file. i.e. `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill/part-00000-9f83fcc5-f5bb-4357-ac95-1528ef51f24d-c000.csv`. I will reference to it as {NULLFILL_PATH}
3. Clear output directory for next step: `hdfs dfs -rm -R hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
4. Run the "Clean" MapReduce job located in /etl_code/brian/clean/: `hadoop jar clean.jar Clean {NULLFILL_PATH} hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
5. Clear output directory for next step: 'hdfs dfs -rm hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv'
6. Copy result of MapReduce: `hdfs dfs -cp hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean/part-r-00000 hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv`

### FX Rates
#### ETL
1. Navigate to the `scripts` directory
2. Make the FX Rates ETL script executable: `chmod +x run_fx_etl.sh`
3. Run the script: `./run_fx_etl.sh`
   * Runs `Clean.scala` and `Drop.scala` to perform ETL on the FX rates dataset.
   * Populates `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl` and `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean` respectively
  
#### Data Profiling
1. Navigate to the `src/etl_code/richard` directory
2. Run `spark-shell --deploy-mode client -i CountRecs.scala` for basic analytics
3. Run `spark-shell --deploy-mode client -i Analysis.scala` for more advanced analytics

### Data Ingest
1. Navigate to the `scripts` directory
2. Make the data ingestion script executable: `chmod +x run_data_ingest.sh`
3. Run the script: `./run_data_ingest.sh`
   * When prompted, enter your Hive namespace (ie. bob123_nyu_edu)
   * Creates `movies`, `fx`, `joined` (`movies` and `fx` joined on `Year` and `Date`), and `norm_joined` (contains normalized currency quantities) tables in the specified namespace
   * Populates `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/joined_data`
  
### Analysis
* Run `spark-shell --deploy-mode client -i src/ana_code/rating_boxoffice_return_analysis.scala` to perform statistical analysis using Spark ML between box office returns and user ratings
  * Results are printed in the Spark console 
