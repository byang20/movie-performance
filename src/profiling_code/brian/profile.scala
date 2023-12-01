import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


// Load CSV file into a DataFrame
val inputFilePath = "hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/movies/clean/clean-IMDb-movies.csv"

val csvOptions = Map("inferSchema" -> "true", "quote" -> "\"", "escape" -> "\"")
var df: DataFrame = spark.read.options(csvOptions).csv(inputFilePath)

df=df.withColumnRenamed("_c0","title").withColumnRenamed("_c1","year").withColumnRenamed("_c2","month").withColumnRenamed("_c3","genres").withColumnRenamed("_c4","duration").withColumnRenamed("_c5","countries").withColumnRenamed("_c6","avg_vote").withColumnRenamed("_c7","num_votes").withColumnRenamed("_c8","budget").withColumnRenamed("_c9","box_office").withColumnRenamed("_c10","currency")

val numericColumns = df.columns.filter(colName => df.schema(colName).dataType.isInstanceOf[org.apache.spark.sql.types.NumericType])

numericColumns.foreach(colName => {

      println(s"Column: $colName")

      val summary = df.select(colName).summary("mean", "50%","stddev").as(colName)
      println(summary.show())
      val mode = df.groupBy(colName).count().orderBy(desc("count")).first()(0)

      
      println(s"Mode: $mode")

      println("\n")
    })


