// Load the dataset, skipping the header
val filePath = "input/ecb-fx-usd-quote.csv"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val data = fullData.filter(row => row != header)

// Removing the first currency column (EUR) and handling nulls
val cleanData = data.map(row => {
  val columns = row.split(",").tail // Skipping the first column (EUR)
  columns.map(col => if (col == "null" || col.isEmpty) "0" else col).mkString(",")
})

// Write the cleaned data to HDFS
val outputPath = "output/clean-ecb_fx_usd_quote.csv"
cleanData.coalesce(1).saveAsTextFile(outputPath)

