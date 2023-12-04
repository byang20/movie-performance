The basic profiling on the movies dataset runs two MapReduce jobs, both called "UniqueRecs". The code for these have already been built in `src/profiling_code/brian/preclean` and `src/profiling_code/brian/postclean`. If you would like to rebuild them, navigate to `src/profiling_code/brian/preclean` and `src/profiling_code/brian/postclean` and run the following commands in each:
1. ``javac -classpath `yarn classpath` -d . UniqueRecsMapper.java``
2. ``javac -classpath `yarn classpath` -d . UniqueRecsReducer.java``
3. ``javac -classpath `yarn classpath`:. -d . UniqueRecs.java``
4. `jar -cvf uniqueRecs.jar *.class`

To perform all the profiling for the movies dataset, run the following commands:

1. Clear output directory for next step: `hdfs dfs -rm -R hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/preclean`
2. Navigate to `src/profiling_code/brian/preclean`  and run the "UniqueRecs" MapReduce job: `hadoop jar uniqueRecs.jar UniqueRecs {NULLFILL_PATH} hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/preclean/`
3. Print results of profiling: `hdfs dfs -cat hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/preclean/part-r-00000`
4. Clear output directory for next step: `hdfs dfs -rm -R hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/postclean`
5. Navigate to `src/profiling_code/brian/postclean/` and run the "UniqueRecs" MapReduce job: `hadoop jar uniqueRecs.jar UniqueRecs hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/postclean`
6. Print results of profiling: `hdfs dfs -cat hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/profiling/postclean/part-r-00000`
7. For more advanced analytics, navigate to `src/profiling_code/brian/` and run `profile.scala`: `spark-shell --deploy-mode client -i profile.scala`
    * Results will print in Spark console
