# Train a Machine Learning Model
In this walkthrough we will connect to a dataset, train a basic machine learning model, and save the model for later use. 

## Configure Azure Storage Account
We will require an Azure Storage Account to read our training dataset. Please note that one can also utilize an Azure Data Lake Store for this same purpose.  

1. Create an [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal#create-a-storage-account-1) using the Azure Portal.  

1. Create a [blob container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container) in the Azure Storage Account you just created. 

1. Download the [dataset](https://bit.ly/2Ezp8dH). We will be using the popular New York Taxi Tips dataset as an example. Please note that our focus here is to show an example of streaming machine learning predictions using Databricks, therefore, the performance of the model may not be optimal. 

1. Using the Azure Portal, upload the taxi dataset to the blob container you created in step 2. 

1. Create a Python notebook in your Azure Databricks workspace to mount the Azure Blob Container to your databricks cluster. Please provide the container name, account name, and account key. Please note that I usually recommend mounting an Azure Data Lake Store to your Databricks cluster to save data and machine learning models, however, for the purpose of this walk through we will use a Blob Container. 
    ```python
    container_name = ""
    account_name = ""
    account_key = ""

    dbutils.fs.mount(
    source = "wasbs://"+container_name+"@"+account_name+".blob.core.windows.net",
    mount_point = "/mnt/" + account_name + "/" + container_name,
    extra_configs = {"fs.azure.account.key."+account_name+".blob.core.windows.net": account_key})
    ```
1.  Test your connection to the blob container by running the following command in your python notebook. It should list all the files available in the container.  
    ```python
    dbutils.fs.ls("/mnt/" + account_name + "/" + container_name)
    ```

## Train a Model
1. In your Azure Databricks workspace create a scala notebook called "TrainModel". 

1. Import required scala libraries. 
    ```scala
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.Pipeline
    ```

1. Read nyc tip data into spark dataframe. Please supply the account name and container name used to mount the blob container. 
    ```scala
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

    // read data
    var df = spark.read.schema(nyc_schema).format("csv").option("header", "true").load("/mnt/" + account_name + "/" + container_name + "/nyctaxitip.csv")
    
    // Create 'label' column
    df = df.withColumn("label", col("tipped"))
    ```

1. You can display the dataframe by running the following command. 
    ```scala
    display(df)
    ```

1. Assemble the training columns into a single vectored column.
    ```scala
    // select data for model and put them in a feature column for training
    var vectorAssembler = new VectorAssembler().setInputCols(Array("passenger_count", "trip_time_in_secs", "trip_distance", "total_amount")).setOutputCol("features")
    var v_df = vectorAssembler.transform(df)
    ```

1. For this example, we will split the data into train and test datasets. We will typically include a 'validation' dataset, however, this is not required for this example. Train datasets are used to train a machine learning model, the validation dataset is used to see how well our model is performing during the development process, and the test dataset is used to evaluate our model prior to deploying it to production or test environments.  
    ```scala
    var splits = v_df.randomSplit(Array(0.75, 0.25))
    var train = splits(0)
    var test = splits(1)
    ```

1. Initialize and train a decision tree classifier.
    ```scala
    val dt = new DecisionTreeClassifier().setMaxDepth(10)

    // Fit the model
    val dt_model = dt.fit(train)
    ```

1. Make predictions on our test dataset.
    ```scala
    var test_predictions = dt_model.transform(test)
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
    - Accuracy:  
    - Precision: 
    - Recall: 
    - F1 Score: 
    ```
    Accuracy: 0.846987
    Precision: 0.82755077
    Recall: 0.8934639
    F1 Score: 0.8592451
    ```

1. Now we must save our model to storage in order to use it a later point in time. Note that we datetime our model so that we are able to save the models we train at any given time, and we save a 'latest' model allowing our streaming prediction script to easily pick up the most current model. 
    ```scala
    // format output paths
    val latest_path = "/mnt/user/blob/rserverdata/public/nycmodels/latest/"
    val date_path = "/mnt/user/blob/rserverdata/public/nycmodels/" + year_str + "/" + month_str + "/" + day_str + "/"
    // save models
    dt_model.write.overwrite().save(latest_path + "nyctaximodel.model")
    dt_model.write.overwrite().save(date_path + "nyctaximodel.model")

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

1. You have no created a script that trains, saves, and tests a machine learning model. This script will ideally be ran on a scheduled or triggered cadence to keep your model up to date as more data is gathered. Since this is written in Databricks you can use the built in job scheduler to run this as you wish. Please move on to [04_ModifyStreamingData](04_ModifyStreamingData.md). 