
// Import libraries
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.Pipeline


// storage account names
account_name = ""
container_name = ""
// Dataset Schema
val nyc_schema = StructType(Array(
        StructField("medallion", StringType),
        StructField("hack_license", StringType),
        StructField("vendor_id", StringType),
        StructField("rate_code", StringType),
        StructField("store_and_fwd_flag", StringType),
        StructField("pickup_datetime", TimestampType),
        StructField("dropoff_datetime", TimestampType),
        StructField("passenger_count", IntegerType),
        StructField("trip_time_in_secs", IntegerType),
        StructField("trip_distance", DoubleType),
        StructField("pickup_longitude", StringType),
        StructField("pickup_latitude", StringType),
        StructField("dropoff_longitude", StringType),
        StructField("dropoff_latitude", StringType),
        StructField("payment_type", StringType),
        StructField("fare_amount", DoubleType),
        StructField("surcharge", DoubleType),
        StructField("mta_tax", DoubleType),
        StructField("tolls_amount", DoubleType),
        StructField("total_amount", DoubleType),
        StructField("tip_amount", DoubleType),
        StructField("tipped", IntegerType),
        StructField("tip_class", IntegerType)
    ))



// read dataframe
var df = spark.read.schema(nyc_schema).format("csv").option("header", "true").load("/mnt/user/blob/"+account_name+"/"+container_name+"/nyctaxitip.csv")
df = df.withColumn("label", col("tipped"))



// select data for model and put them in a feature column for training
var vectorAssembler = new VectorAssembler().setInputCols(Array("passenger_count", "trip_time_in_secs", "trip_distance", "total_amount")).setOutputCol("features")



// split the dataset 
var splits = df.randomSplit(Array(0.75, 0.25))
var train = splits(0)
var test = splits(1)



// pipeline train model here
val dt = new DecisionTreeClassifier().setMaxDepth(10)
val pipeline = new Pipeline()
  .setStages(Array(vectorAssembler, dt))

val pipelineModel = pipeline.fit(train)

// make predictions on test
var test_predictions = pipelineModel.transform(test)

// get eval vars
val tp = test_predictions.filter($"label" === 1 && $"prediction" === 1).count().toFloat
val tn = test_predictions.filter($"label" === 0 && $"prediction" === 0).count().toFloat
val fp = test_predictions.filter($"label" === 0 && $"prediction" === 1).count().toFloat
val fn = test_predictions.filter($"label" === 1 && $"prediction" === 0).count().toFloat


// calculate metrics
val accuracy = (tn + tp)/(tn + tp + fn + fp)
val precision = (tp) / (tp + fp)
val recall = (tp) / (tp + fn)
val f1 = 2*((precision*recall)/(precision+recall))

println("Accuracy: " + accuracy)
println("Precision: " + precision)
println("Recall: " + recall)
println("F1 Score: " + f1)



// date values
val year_str = java.time.LocalDate.now.getYear.toString
val month_str = java.time.LocalDate.now.getMonthValue.toString
val day_str = java.time.LocalDate.now.getDayOfMonth.toString

// set folder paths
val latest_path = "/mnt/user/blob/"+account_name+"/"+container_name+"/nycmodels/latest/"
val date_path = "/mnt/user/blob/"+account_name+"/"+container_name+"/nycmodels/" + year_str + "/" + month_str + "/" + day_str + "/"



// save model to date folder and latest folder
pipelineModel.write.overwrite().save(latest_path + "nyctaximodel.model")
pipelineModel.write.overwrite().save(date_path + "nyctaximodel.model")



// create eval dataframe
case class Eval(ModelName: String, Date: String, Accuracy: Float, Precision: Float, Recall: Float, F1Score: Float)

val eval1 = new Eval("NYCModel", java.time.LocalDate.now.toString, accuracy, precision, recall, f1)
val seq1 = Seq(eval1)
val eval_df = seq1.toDF()
display(eval_df)



// save eval dataframe
eval_df.write.mode(SaveMode.Overwrite).parquet(latest_path + "nyc_eval.parquet")
eval_df.write.mode(SaveMode.Overwrite).parquet(date_path + "nyc_eval.parquet")




