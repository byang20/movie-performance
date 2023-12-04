The second part of the ETL on the movies dataset runs a MapReduce job called "Clean". The code for this has already been built in `src/etl_code/brian/clean`. If you would like to rebuild it, navigate to `src/etl_code/brian/clean` and run the following commands:
1. ``javac -classpath `yarn classpath` -d . CleanMapper.java``
2. ``javac -classpath `yarn classpath` -d . CleanReducer.java``
3. ``javac -classpath `yarn classpath`:. -d . Clean.java``
4. `jar -cvf clean.jar *.class`

To run the ETL code for movies, follow these steps:

1. Navigate to `src/etl_code/brian/nullfill/` and run `nullfill.scala`: `spark-shell --deploy-mode client -i nullfill.scala`
   * Pulls original data set from `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/input/IMDb-movies.csv`
   * Replaces blank fields with String "Null" and saves result to `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill`
2. Run `hdfs dfs -ls hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill` to find results. Take note of path of the file (e.x. `hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill/part-00000-9f83fcc5-f5bb-4357-ac95-1528ef51f24d-c000.csv`). I will reference to it as {NULLFILL_PATH}
3. Clear output directory for next step: `hdfs dfs -rm -R hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
4. Navigate to `src/etl_code/brian/clean/` and run the "Clean" MapReduce job: `hadoop jar clean.jar Clean {NULLFILL_PATH} hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean`
5. Clear output directory for next step: `hdfs dfs -rm hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv`
6. Copy result of MapReduce to fixed path for future use: `hdfs dfs -cp hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/clean/part-r-00000 hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv`
