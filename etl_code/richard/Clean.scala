// Load CSV into RDD, skipping header and first row
val filePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/fx/input/ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val tmpData = fullData.filter(row => row != header)
val secondRow = tmpData.first()
val data = tmpData.filter(row => row != secondRow)

// Replace null values with 0.0
val cleanData = data.map { row =>
  row.split(",").map(value => if (value == "NaN") "0.0" else value).mkString(",")
}

// Collect the cleaned data
val collectedCleanData = cleanData.collect()

// Create a new array with the header
val finalDataArray = header +: collectedCleanData

// Convert the array back to an RDD
val finalData = sc.parallelize(finalDataArray)

// Write the final cleaned data to HDFS
val outputPath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/fx/etl/etl-ecb-fx-usd-quote.csv"
finalData.coalesce(1).saveAsTextFile(outputPath)

