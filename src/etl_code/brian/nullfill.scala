import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


// Load CSV file into a DataFrame
val inputFilePath = "hdfs://nyu-dataproc-m/user/bay2006_nyu_edu/nullfill/IMDb-movies.csv"
val outputFilePath = "nullfill/nullfill-IMDb-movies"

val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "quote" -> "\"", "escape" -> "\"")
var df: DataFrame = spark.read.options(csvOptions).csv(inputFilePath)

val stringColumns = df.columns.filter(colName => df.schema(colName).dataType == org.apache.spark.sql.types.StringType)
val numericColumns = df.columns.filter(colName => df.schema(colName).dataType.isInstanceOf[org.apache.spark.sql.types.NumericType])

df = stringColumns.foldLeft(df)((df, colName) => df.withColumn(colName, when(col(colName).isNull, "Null").otherwise(col(colName))))
df = numericColumns.foldLeft(df)((df, colName) => df.withColumn(colName, when(col(colName).isNull, "Null").otherwise(col(colName))))


df = df.coalesce(1)

// Save the result as a CSV file
df.write
  .mode("overwrite")  // Overwrite the output file if it already exists
  .options(csvOptions)
  .csv(outputFilePath)
