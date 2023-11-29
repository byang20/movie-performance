#!/bin/bash

# Step 1: Create a local directory for FX ETL and Profiling files
mkdir fx_etl_profiling_files
cd fx_etl_profiling_files

# Step 2: Fetch Scala files from HDFS
hdfs dfs -copyToLocal project/src/etl_code/richard//Clean.scala .
hdfs dfs -copyToLocal project/src/etl_code/richard/CountRecs.scala .
hdfs dfs -copyToLocal project/src/profiling_code/richard/Drop.scala .
hdfs dfs -copyToLocal project/src/profiling_code/richard/Analysis.scala .

# Step 3: Remove previous output files
hdfs dfs -rm -r project/data/fx/etl
hdfs dfs -rm -r project/data/fx/clean

# Step 4: Run ETL files in Spark shell
spark-shell --deploy-mode client -i <<EOF
:load Clean.scala
:quit
EOF
spark-shell --deploy-mode client -i <<EOF
:load Drop.scala
:quit
EOF

# Step 5: Echo that the process is done
echo "All Spark jobs have completed."
