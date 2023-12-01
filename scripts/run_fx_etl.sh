#!/bin/bash

# Remove previous output directories
hdfs dfs -rm -r project/data/fx/etl
hdfs dfs -rm -r project/data/fx/clean

# Run ETL files in Spark shell
spark-shell --deploy-mode client -i <<EOF
:load ../src/etl_code/richard/Clean.scala
:quit
EOF
spark-shell --deploy-mode client -i <<EOF
:load ../src/profiling_code/richard/Drop.scala
:quit
EOF

echo "All Spark jobs have completed."
