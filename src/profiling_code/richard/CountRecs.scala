// Load CSV into RDD, skipping header and first row
val filePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/input/ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val tmpData = fullData.filter(row => row != header)
val firstRow = tmpData.first()
val data = tmpData.filter(row => row != firstRow)

// Count number of records
val totalCount = data.count()
println(s"Total number of records: $totalCount")

// Map records to a key (Date) and value (Currencies) and count the number of records using map() function
val mapData = data.map(row => {
  val columns = row.split(",")
  (columns(0), columns.tail.mkString(","))
})
val mapCount = mapData.count()
println(s"Number of records after mapping: $mapCount")

// Find the distinct values in each column
val columnNames = header.split(",")
columnNames.zipWithIndex.foreach { case (colName, colIndex) =>
  val distinctCount = data.map(row => row.split(",")(colIndex)).distinct().count()
  println(s"Column $colName: $distinctCount distinct values")
}

System.exit(0)