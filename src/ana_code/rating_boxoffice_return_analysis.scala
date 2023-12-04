import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator


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

// Prepare data for model training and evaluation
val featureCols = Array("boxoffice_return")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features").setHandleInvalid("skip")
val data = assembler.transform(df).select($"features", $"rating".as("label")).na.drop()
val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = 42)


// Define linear regression model, train model, make predictions
val lr = new LinearRegression()
val lrModel = lr.fit(trainData)
val lrPredictions = lrModel.transform(testData)

// Evaluate the model
val lrEvaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")
val lrMAE = lrEvaluator.evaluate(lrPredictions)
println(s"Linear Regression - MAE: $lrMAE")


// Define random forest model, train model, make predictions
val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
val rfModel = rf.fit(trainData)
val rfPredictions = rfModel.transform(testData)

// Evaluate the model
val rfEvaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")
val rfMAE = rfEvaluator.evaluate(rfPredictions)
println(s"Random Forest Regression - MAE: $rfMAE")

System.exit(0)