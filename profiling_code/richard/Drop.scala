// Load CSV into RDD, removing header
val filePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/etl/etl-ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val dataWithoutHeader = fullData.filter(row => row != header)

// Function to transform each row
def transformRow(row: String): String = {
  val columns = row.split(",")
  val dateParts = columns(0).split("-")
  val year = dateParts(0)
  val month = dateParts(1)
  
  // Construct the new row with Year and Month replacing Date
  Array(year, month) ++ columns.tail mkString ","
}

// Apply transformation to data
val transformedData = dataWithoutHeader.map(transformRow).collect()

// Adjust header
val newHeader = "Year,Month" + header.substring(header.indexOf(","))

// Combine header with transformed data
val finalDataArray = Seq(newHeader) ++ transformedData

// Convert array back to an RDD
val finalData = sc.parallelize(finalDataArray)

// Write the final data to HDFS
val outputPath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/fx/clean/clean-ecb-fx-usd-quote.csv"
finalData.coalesce(1).saveAsTextFile(outputPath)

