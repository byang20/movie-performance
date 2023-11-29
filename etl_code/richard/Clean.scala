// Load CSV into RDD
val filePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/input/ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)

// Remove header and first row
val header = fullData.first()
val tmpData = fullData.filter(row => row != header)
val secondRow = tmpData.first()
val cleanData = tmpData.filter(row => row != secondRow)

// Collect the cleaned data
val collectedCleanData = cleanData.collect()

// Create a new array with the header
val finalDataArray = header +: collectedCleanData

// Convert the array back to an RDD
val finalData = sc.parallelize(finalDataArray)

// Write the final cleaned data to HDFS
val outputPath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/fx/etl/etl-ecb-fx-usd-quote.csv"
finalData.coalesce(1).saveAsTextFile(outputPath)

