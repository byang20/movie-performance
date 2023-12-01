#!/bin/bash

mkdir data_ingest_files
cd data_ingest_files

# Ask for namespace or take it as an argument
if [ "$#" -eq 1 ]; then
    namespace=$1
else
    read -p "Enter the namespace for Hive execution: " namespace
fi

# JDBC connection string
jdbc_url="jdbc:hive2://localhost:10000/$namespace"

# List of SQL files to execute
sql_files=("../src/data_ingest/movies_fx_tables.sql" "../src/data_ingest/join_movies_fx.sql" "../src/data_ingest/normalize_joined.sql")

# Loop through SQL files and execute them in Beeline
for sql_file in "${sql_files[@]}"
do
    echo "Running $sql_file in the $namespace database..."
    beeline -u "$jdbc_url" -f $sql_file
done

echo "All SQL scripts have been executed."
