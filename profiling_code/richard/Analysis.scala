// Load CSV into RDD, removing header
val filePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/fx/etl"
val fullData = sc.textFile(filePath)
val header = fullData.first()
val dataWithoutHeader = fullData.filter(row => row != header)

// Split each line and remove the date column
val columns = dataWithoutHeader.map(_.split(",")).map(_.tail)

// Get RDDs for each column
val headerColumns = header.split(",").tail // Exclude the date column
val numberOfColumns = headerColumns.length
val columnRDDs = (1 until numberOfColumns).map(i => 
  dataWithoutHeader.map(_.split(",")(i).toDouble)
)

import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD

def computeStatistics(columnRDD: RDD[Double]): (Double, Double, Double, Double) = {
  // Filter out NaN values
  val filteredColumnRDD = columnRDD.filter(!_.isNaN)

  // Aggregate using StatCounter for mean and standard deviation
  val stats = filteredColumnRDD.aggregate(StatCounter())(
    (acc, value) => acc.merge(value),
    (acc1, acc2) => acc1.merge(acc2)
  )

  val mean = stats.mean
  val stdev = stats.stdev

  // Calculate median
  val sortedData = filteredColumnRDD.sortBy(identity).collect()
  val count = sortedData.length
  val median = if (count % 2 == 0) {
    (sortedData(count / 2 - 1) + sortedData(count / 2)) / 2.0
  } else {
    sortedData(count / 2)
  }

  // Calculate mode
  val mode = filteredColumnRDD
    .map(value => (value, 1))
    .reduceByKey(_ + _)
    .map(_.swap)
    .max()(Ordering.by[(Int, Double), Int](_._1))
    ._2

  (mean, median, mode, stdev)
}

// Apply function to each column
val columnStats = columnRDDs.map(computeStatistics)
headerColumns.zip(columnStats).foreach { case (columnName, (mean, median, mode, stdev)) =>
  println(s"Column: $columnName")
  println(f"Mean: $mean%.2f, Median: $median%.2f, Mode: $mode%.2f, Standard Deviation: $stdev%.2f")
  println("-" * 50) // Separator for readability
}

