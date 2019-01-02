# Train a Machine Learning Model
In this walkthrough we will connect to a dataset, train a basic machine learning model, and save the model for later use. A copy of the script we implement can be found [here](../../code/TrainTaxiModel.scala).

**Please make sure you are no longer sending data to your event hub using the "SendData" Python notebook**

## Configure Azure Storage Account
We will require an Azure Storage Account to read our training dataset. Please note that one can also utilize an Azure Data Lake Store for this same purpose.  

1. Create an [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal#create-a-storage-account-1) using the Azure Portal.  

1. Create a [blob container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container) in the Azure Storage Account you just created. 

1. Download the [dataset](https://bit.ly/2Ezp8dH). We will be using the popular New York Taxi Tips dataset as an example. Please note that our focus here is to show an example of streaming machine learning predictions using Databricks, therefore, the performance of the model may not be optimal. 

1. Using the Azure Portal, upload the taxi dataset to the blob container you created in step 2. 

1. Create a new Python notebook called "MountContainer" in your Azure Databricks workspace to mount the Azure Blob Container to your databricks cluster. Please provide the container name, account name, and account key. Please note that I usually recommend mounting an Azure Data Lake Store to your Databricks cluster to save data and machine learning models, however, for the purpose of this walk through we will use a Blob Container. 
    ```python
    container_name = ""
    account_name = ""
    account_key = ""

    dbutils.fs.mount(
    source = "wasbs://"+container_name+"@"+account_name+".blob.core.windows.net",
    mount_point = "/mnt/" + account_name + "/" + container_name,
    extra_configs = {"fs.azure.account.key."+account_name+".blob.core.windows.net": account_key})
    ```
1.  Test your connection to the blob container by running the following command in your python notebook. It should list all the files available in the container, which should just be the dataset you uploaded previously.  
    ```python
    dbutils.fs.ls("/mnt/" + account_name + "/" + container_name)
    ```

## Train a Model
In order to make streaming predictions on data we will need to first train a model to transform and predict the data. We will be using  ML Pipelines to train and predict streaming data. A Pipeline is a sequence of stages, where each stage is either a transformation or a model estimator. These stages are run in order, and the input DataFrame is transformed as it passes through each stage. For transformation stage, the transform() method is called on the DataFrame, and for each estimator the fit() method is called. This allows us to box up our data transformations into a single line of code to easily make predictions on a stream. 

1. In your Azure Databricks workspace create a scala notebook called "TrainModel". 

1. Import required scala libraries. 
    ```scala
    // import libraries
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.classification.DecisionTreeClassifier
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.Pipeline
    ```

1. Read nyc tip data into spark dataframe. Please supply the account name and container name used to mount the blob container. 
    ```scala
    // set your input storage account name and container
    val account_name = ""
    val container_name = ""
    // schema of csv
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

    // read data from blob storage
    var df = spark.read.schema(nyc_schema).format("csv").option("header", "true").load("/mnt/" + account_name + "/" + container_name + "/nyctaxitip.csv")
    
    // Create 'label' column that we want to predict
    df = df.withColumn("label", col("tipped"))
    ```

1. You can display the dataframe by running the following command. 
    ```scala
    display(df)
    ```

1. Assemble the training columns into a single vectored column. This is our input column for training a model. 
    ```scala
    // select data for model and put them in a feature column for training
    var vectorAssembler = new VectorAssembler().setInputCols(Array("passenger_count", "trip_time_in_secs", "trip_distance", "total_amount")).setOutputCol("features")
    ```

1. For this example, we will split the data into train and test datasets. We will typically include a 'validation' dataset, however, this is not required for this example. Train datasets are used to train a machine learning model, the validation dataset is used to see how well our model is performing during the development process, and the test dataset is used to evaluate our model prior to deploying it to production or test environments to ensure we have not overfit our model to the training or validation sets.  
    ```scala
    var splits = df.randomSplit(Array(0.75, 0.25))
    var train = splits(0)
    var test = splits(1)
    ```

1. Next we need to create our pipeline model and train it. In this step we initialize a decision tree classifier, and a pipeline object. The pipeline object will first create the vector column of the data we want to use for predictions, and then it will train a decision tree model.  
    ```scala
    // initialize a decision tree classifier
    val dt = new DecisionTreeClassifier().setMaxDepth(10)
    // set up our ml pipeline
    val pipeline = new Pipeline()
    .setStages(Array(vectorAssembler, dt))
    // train a pipeline model 
    val pipelineModel = pipeline.fit(train)
    ```

1. Make predictions on our test dataset.
    ```scala
    // make predictions on our dataset
    var test_predictions = pipelineModel.transform(test)
    ```

1.  We now need to evaluate our test dataset to understand how well the model is performing. 
    ```scala
    // calculate true positives/negatives and false positives/negatives
    val tp = test_predictions.filter($"label" === 1 && $"prediction" === 1).count().toFloat
    val tn = test_predictions.filter($"label" === 0 && $"prediction" === 0).count().toFloat
    val fp = test_predictions.filter($"label" === 0 && $"prediction" === 1).count().toFloat
    val fn = test_predictions.filter($"label" === 1 && $"prediction" === 0).count().toFloat

    // calculate metrics and print the values to console
    val accuracy = (tn + tp)/(tn + tp + fn + fp)
    val precision = (tp) / (tp + fp)
    val recall = (tp) / (tp + fn)
    val f1 = 2*((precision*recall)/(precision+recall))

    println("Accuracy: " + accuracy)
    println("Precision: " + precision)
    println("Recall: " + recall)
    println("F1 Score: " + f1)
    ```

1. Below is the sample output from running the above command. Please note the following descriptions of the evaluaton metrics:
    ```
    Accuracy: 0.846987
    Precision: 0.82755077
    Recall: 0.8934639
    F1 Score: 0.8592451
    ```

1. Now we must save our model to storage in order to use it a later point in time. Note that we datetime our model so that we are able to save the models we train at any given time, and we save a 'latest' model allowing our streaming prediction script to easily pick up the most current model. 
    ```scala
    // format output paths
    val latest_path = "/mnt/" + account_name + "/" + container_name + "/nycmodels/latest/"
    val date_path = "/mnt/" + account_name + "/" + container_name + "/nycmodels/" + year_str + "/" + month_str + "/" + day_str + "/"
    // save models
    pipelineModel.write.overwrite().save(latest_path + "nyctaximodel.model")
    pipelineModel.write.overwrite().save(date_path + "nyctaximodel.model")

    ```
1. We will also format and save our evaluation criteria, allowing us to create reports to see the performance of our data over time. 
    ```scala 
    // format evaluation criteria as a dataframe
    case class Eval(ModelName: String, Date: String, Accuracy: Float, Precision: Float, Recall: Float, F1Score: Float)

    val eval1 = new Eval("NYCModel", java.time.LocalDate.now.toString, accuracy, precision, recall, f1)
    val seq1 = Seq(eval1)
    val eval_df = seq1.toDF()
    // save our evaluation dataframe
    eval_df.write.mode(SaveMode.Overwrite).parquet(latest_path + "nyc_eval.parquet")
    eval_df.write.mode(SaveMode.Overwrite).parquet(date_path + "nyc_eval.parquet")

    display(eval_df)// display dataframe if wanted
    ```

1. You have now created a script that trains, saves, and tests a machine learning model. This script will ideally be ran on a scheduled or triggered cadence to keep your model up to date as more data is gathered. Since this is written in Databricks you can use the built in job scheduler to run this as you wish. Please move on to [04_ModifyStreamingData](04_ModifyStreamingData.md). 