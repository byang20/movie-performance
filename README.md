# Movie Success Analysis
NYU Processing Big Data for Analytics Applications Project

## Data
The data used for this project can be found in the following links.

1. https://data.world/mahe432/movies
2. https://data.world/hdx/3c7aca84-e00b-46a0-a8d5-d769b16e5351

## How To Run
*This assumes that you are affiliated with NYU and are able to access the university's High Performance Cluster (HPC) and have access to the root directory of the project in HDFS.*

### Movies

### FX Rates
#### ETL
1. Copy `scripts/run_fx_etl.sh` to your local directory
3. Make the script executable: `chmod +x run_fx_etl.sh`
4. Run the script: `./run_fx_etl.sh`
   * Creates a new directory `fx_etl_profiling_files` and copies relevant Scala scripts from HDFS
   * Runs `Clean.scala` and `Drop.scala` to perform ETL on the FX rates dataset.
  
#### Data Profiling
1. Navigate into `fx_etl_profiling_files`
2. Run `spark-shell --deploy-mode client -i CountRecs.scala` for basic analytics
3. Run `spark-shell --deploy-mode client -i Analysis.scala` for more advanced analytics

