1. Run `/etl_code/brian/nullfill/nullfill.scala`: `spark-shell --deploy-mode client -i nullfill.scala`
   * Pulls original data set from `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/input/IMDb-movies.csv`
   * Replaces blank fields with String "Null" and saves result to `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill`
2. Run `hdfs dfs -ls hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill` to find results, and take note of path of the file (i.e. `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill/part-00000-9f83fcc5-f5bb-4357-ac95-1528ef51f24d-c000.csv`). I will reference to it as {NULLFILL_PATH}
3. Clear output directory for next step: `hdfs dfs -rm -R hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
4. Run the "Clean" MapReduce job located in `/etl_code/brian/clean/`: `hadoop jar clean.jar Clean {NULLFILL_PATH} hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
5. Clear output directory for next step: `hdfs dfs -rm hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv`
6. Copy result of MapReduce: `hdfs dfs -cp hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean/part-r-00000 hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv`