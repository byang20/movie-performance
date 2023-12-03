var df = spark.read.option("inferSchema", "true").csv("hdfs://nyu-dataproc-m/user/rz2123_nyu_edu/project/data/joined_data")
df = df.toDF(
  "title", 
  "year", 
  "month", 
  "genres", 
  "duration", 
  "countries", 
  "rating", 
  "votes", 
  "budget", 
  "boxoffice", 
  "currency", 
  "exchange_rate", 
  "norm_budget", 
  "norm_boxoffice", 
  "boxoffice_return"
)

// Explode genres and countries
df = df.withColumn("genre", explode(split(col("genres"), ", ")))
df = df.withColumn("country", explode(split(col("countries"), ", ")))


// Select only necessary columns
df = df.select("genre", "country", "rating", "boxoffice_return").na.drop()

df = df.filter(col("boxoffice_return").cast("double") <= 100)


val countryRes = df.groupBy("country").agg(avg("rating").alias("avg_rating"), avg("boxoffice_return").alias("avg_boxoffice_return"))
val genreRes = df.groupBy("genre").agg(avg("rating").alias("avg_rating"), avg("boxoffice_return").alias("avg_boxoffice_return"))


println("Highest avg ratings by country")
countryRes.orderBy(desc("avg_rating")).select("country", "avg_rating").show(5)
println("Lowest avg ratings by country")
countryRes.orderBy("avg_rating").select("country", "avg_rating").show(5)
println("Highest avg box office returns by country")
countryRes.orderBy(desc("avg_boxoffice_return")).select("country", "avg_boxoffice_return").show(5)
println("Lowest avg box office returns by country")
countryRes.orderBy("avg_boxoffice_return").select("country", "avg_boxoffice_return").show(5)

println("Highest avg ratings by genre")
genreRes.orderBy(desc("avg_rating")).select("genre", "avg_rating").show(5)
println("Lowest avg ratings by genre")
genreRes.orderBy("avg_rating").select("genre", "avg_rating").show(5)
println("Highest avg box office returns by genre")
genreRes.orderBy(desc("avg_boxoffice_return")).select("genre", "avg_boxoffice_return").show(5)
println("Lowest avg box office returns by genre")
genreRes.orderBy("avg_boxoffice_return").select("genre", "avg_boxoffice_return").show(5)
