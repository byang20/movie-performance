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

To run analytics, first perform ETL for the Movies dataset, then perform ETL for the FX Rates datset, and finally perform data ingestion. These steps are necessary to populate the directories used for analytics.

### Movies
#### ETL
Reference `scripts/movies_etl_instructions.md` for instructions to perform ETL on the Movies dataset. 

#### Data Profiling
Reference `scripts/movies_profiling_instructions.md`for instructions to perform data profiling on the Movies dataset. 

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
* Run `spark-shell --deploy-mode client -i src/ana_code/country_genre_analysis.scala` to compute highest and lowest average ratings and box office returns for distinct countries and genres
  * Results are printed in the Spark console
