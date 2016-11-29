import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.regression.LinearRegression

object LinearRegression extends App {

  import org.apache.log4j._
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Create Session App
  val spark = SparkSession.builder().master("local")
    .appName("LinearRegressionExample")
    .getOrCreate()

  // Use Spark to read in the Ecommerce_customers csv file.
  val data = spark.read.option("header","true").option("inferSchema","true")
    .format("com.databricks.spark.csv").load("src/main/resources/Ecommerce_customers")


  // Print the Schema of the DataFrame
  data.printSchema()

  // Print out the first row
  val colnames = data.columns
  val firstrow = data.head(1)(0)
  println("\n")
  println("Example Data Row")
  for(ind <- Range(1,colnames.length)){
    println(colnames(ind))
    println(firstrow(ind))
    println("\n")
  }

  ////////////////////////////////////////////////////
  //// Setting Up DataFrame for Machine Learning ////
  //////////////////////////////////////////////////

  import org.apache.spark.ml.feature.VectorAssembler
  import org.apache.spark.ml.linalg.Vectors


  // This is needed to use the $-notation
  import spark.implicits._
  val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",
                            $"Time on App",$"Time on Website",$"Length of Membership")

  // An assembler converts the input values to a vector
  // A vector is what the ML algorithm reads to train a model
  val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")

  // Use the assembler to transform our DataFrame to the two columns: label and features
  val output = assembler.transform(df).select($"label",$"features")

  // Create a Linear Regression Model object
  val lr = new LinearRegression()

  // Fit the model to the data and call this model lrModel
  val lrModel = lr.fit(output)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics!
  // Use the .summary method off your model to create an object
  // called trainingSummary
  val trainingSummary = lrModel.summary

  // Show the residuals, the RMSE, the MSE, and the R^2 Values.
  trainingSummary.residuals.show()

  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"MSE: ${trainingSummary.meanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

  // Stop spark
  spark.stop()
}