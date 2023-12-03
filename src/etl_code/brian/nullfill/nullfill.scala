import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Load CSV file into a DataFrame
val inputFilePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/input/IMDb-movies.csv"
val outputFilePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/etl/nullfill"

var csvOptions = Map("header" -> "true", "inferSchema" -> "true", "quote" -> "\"", "escape" -> "\"")
var df: DataFrame = spark.read.options(csvOptions).csv(inputFilePath)

println(s"Input successfully read from $inputFilePath")

val stringColumns = df.columns.filter(colName => df.schema(colName).dataType == org.apache.spark.sql.types.StringType)
val numericColumns = df.columns.filter(colName => df.schema(colName).dataType.isInstanceOf[org.apache.spark.sql.types.NumericType])

df = stringColumns.foldLeft(df)((df, colName) => df.withColumn(colName, when(col(colName).isNull, "Null").otherwise(col(colName))))
df = numericColumns.foldLeft(df)((df, colName) => df.withColumn(colName, when(col(colName).isNull, "Null").otherwise(col(colName))))


df = df.coalesce(1)

csvOptions = Map("header" -> "false", "inferSchema" -> "true", "quote" -> "\"", "escape" -> "\"")

// Save the result as a CSV file
df.write
  .mode("overwrite")  // Overwrite the output file if it already exists
  .options(csvOptions)
  .csv(outputFilePath)

println(s"Output successfully written to $outputFilePath")

System.exit(0)
