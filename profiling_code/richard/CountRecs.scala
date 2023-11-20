// Load CSV into RDD, skipping the header row
val filePath = "input/ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val data = fullData.filter(row => row != header)

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
val columnNames = header.split(",").tail // Excluding the Date column
columnNames.zipWithIndex.foreach { case (colName, colIndex) =>
  val distinctCount = data.map(row => row.split(",")(colIndex + 1)).distinct().count()
  println(s"Column $colName: $distinctCount distinct values")
}

