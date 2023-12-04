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

// Filter out movies with extreme returns
df = df.filter(col("boxoffice_return").cast("double") <= 100)

// Find countries with less than 30 records
// Filter out these countries from the original dataset
// Calculate average returns and ratings for each country remaining
val countryCounts = df.groupBy("country").count().filter(col("count") > 30)
val filteredCountry = df.join(countryCounts, "country")
val countryRes = filteredCountry.groupBy("country").agg(avg("rating").alias("avg_rating"), avg("boxoffice_return").alias("avg_boxoffice_return"))

// Find genres with less than 30 records
// Filter out these genres from the original dataset
// Calculate average returns and ratings for each genre remaining
val genreCounts = df.groupBy("genre").count().filter(col("count") > 30)
val filteredGenre = df.join(genreCounts, "genre")
val genreRes = filteredGenre.groupBy("genre").agg(avg("rating").alias("avg_rating"), avg("boxoffice_return").alias("avg_boxoffice_return"))

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

System.exit(0)
