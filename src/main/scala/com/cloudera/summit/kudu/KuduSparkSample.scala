package com.cloudera.summit.kudu

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.kududb.client._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.kududb.spark.kudu._
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._


/**
  * Kudu Spark Sample
  *
  * Performs insert, update and scan operations on a Kudu table
  * in Spark.
  *
  */
object KuduSparkSample {

  // Case class defined with the same schema (column names and types) as
  // the kudu table we will be writing into
  case class Customer(name:String, age:Int, city:String)

  def main(args: Array[String]): Unit = {

    // Setup Spark configuration and related contexts
    val sparkConf = new SparkConf()
      .setAppName("Kudu Spark Sample")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // This allows us to implicitly convert RDD to a DataFrame
    import sqlContext.implicits._

    val kuduMaster = "ip-10-13-4-52.ec2.internal:7051"

    ///
    /// KUDU CONTEXT
    ///

    // Always start with a KuduContext defined, passing in a simple
    // comma-separated String, listing of all the Kudu Masters with
    // port numbers
    val kuduContext = new KuduContext(kuduMaster)

    ///
    /// TABLE EXISTS AND DROP
    ///

    var kuduTableName = "spark_kudu_tbl"

    // Check if the table exists, and drop it if it does
    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }

    ///
    /// CREATE TABLE
    ///

    // 1. Give your table a name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define a schema
    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("name", StringType, false) ::
        StructField("age", IntegerType, true) ::
        StructField("city", StringType, true) :: Nil)

    // 3. Define the primary key
    val kuduPrimaryKey = Seq("name")



    // 4. Specify any further options
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions
      .setRangePartitionColumns(List("name").asJava)
      .setNumReplicas(3)

    // 5. Call create table API
    kuduContext.createTable(
      // Table name, schema, primary key and options
      kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)


    ///
    /// WRITING TO TABLE - KUDU CONTEXT
    ///

    // Ideally, we prepare the set of content we want to write to the kudu
    // table by preparing a DataFrame with content to be written.
    //
    // DataFrames can be constructed from structured data files, Hive tables,
    // external databases, or existing RDDs.
    //
    // For the sake of simplicity, we will create a simple RDD, then
    // convert it into a DataFrame which we will use to write to the table.

    // Define a list of customers based on the case class already defined above
    val customers = Array(
      Customer("jane", 30, "new york"),
      Customer("jordan", 18, "toronto"),
      Customer("michael", 46, "denver"))

    // Create RDD out of the customers Array
    val customersRDD = sc.parallelize(customers)

    // Now, using reflection, this RDD can easily be converted to a DataFrame
    // Ensure to do the :
    //     import sqlContext.implicits._
    // above to have the toDF() function available to you
    val customersDF = customersRDD.toDF()

    // Let's write this prepared DataFrame into our kudu table
    kuduContext.writeRows(customersDF, kuduTableName, false)

    ///
    /// UPDATING A TABLE - KUDU CONTEXT
    ///

    // Similar to writing, we want to prepare a DataFrame that will have
    // updated content.
    // We create our DataFrame similar to above.
    val customersModified = Array(
      Customer("jane", 30, "chicago"),
      Customer("jordan", 21, "toronto"))

    val customersModifiedDF = sc.parallelize(customersModified).toDF()

    // Update the rows ensuring that the 3rd parameter is set to true,
    // otherwise an exception occurs.
    kuduContext.writeRows(customersModifiedDF, kuduTableName, true)


    ///
    /// READING FROM TABLE : NATIVE RDD
    ///

    // We can read from our table by simply making an RDD, which will,
    // under the covers make use of the native kudu reader, rather than
    // an input format typically used in MapReduce jobs

    // 1. Specify a table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Specify the columns you want to project
    val kuduTableProjColumns = Seq("name", "age")

    // 3. Read table, represented now as RDD
    val custRDD = kuduContext.kuduRDD(sc, kuduTableName, kuduTableProjColumns)

    // We get a RDD[Row] coming back to us. Lets send through a map to pull
    // out the name and age into the form of a tuple
    val custTuple = custRDD.map { case Row(name: String, age: Int) => (name, age) }

    // Print it on the screen just for fun
    custTuple.collect().foreach(println(_))

    ///
    /// WRITING TO TABLE - DATAFRAME
    ///

    // We create a DataFrame, and this time write to the table using
    // the DataFrame API directly, treating the kudu table as a data source.

    val customerSample = Array(
      Customer("harry", 28, "winnipeg"),
      Customer("frank", 52, "dallas"),
      Customer("gina", 23, "sarasota"))

    // Create our DataFrame
    val customerSampleDF = sc.parallelize(customerSample).toDF()

    // Specify the table name
    kuduTableName = "spark_kudu_tbl"

    // Prepare options that represent our table and kudu cluster
    val kuduDataFrameOptions: Map[String, String] = Map(
      "kudu.table" -> kuduTableName,
      "kudu.master" -> kuduMaster)

    // Call our write method, sending in our table/kudu master information
    // together with 'append' mode so we can write.
    customerSampleDF.write.options(kuduDataFrameOptions).mode("append").kudu

    ///
    /// UPDATING TABLE - DATAFRAME
    ///

    // We create a DataFrame that will update existing rows we've inserted
    // above.

    // Create our DataFrame
    val customerUpdateDF = sc.parallelize(Array(
      Customer("harry", 23, "calgary"))).toDF()

    // Specify the table name
    kuduTableName = "spark_kudu_tbl"

    // Prepare options that represent our table and kudu cluster
    val kuduDFUpdateOptions: Map[String, String] = Map(
      "kudu.table" -> kuduTableName,
      "kudu.master" -> kuduMaster)

    // Call our update method, sending in our table/kudu master information
    // together with 'overwrite' mode so we can update.
    customerUpdateDF.write.options(kuduDFUpdateOptions).mode("overwrite").kudu

    ///
    /// READING FROM TABLE : DATA FRAME
    ///

    // We now go through several examples of reading data in using Spark SQL and
    // Data Frames.

    // Specify a table name
    kuduTableName = "spark_kudu_tbl"

    // Start by specifying the table and cluster we're reading from as a Map
    // which will be passed into our SQL Context
    val kuduDFReadOptions = Map(
      "kudu.table" -> kuduTableName,
      "kudu.master" -> kuduMaster
    )

    // Read our table into a DataFrame
    val customerReadDF = sqlContext.read.options(kuduDFReadOptions).kudu

    // Show our table to the screen.
    customerReadDF.show()

    // Read our table now, specifying the projected columns
    val customerNameCityDF = sqlContext.read.options(kuduDFReadOptions).kudu
      .select("name", "city").show()

    // Register our table as a temp table so we can use Spark SQL directly
    // on this table

    // Pick a name for our temporary table
    val kuduTempTableName = "customer"

    // Register the temp table
    sqlContext.read.options(kuduDFReadOptions).kudu
      .registerTempTable(kuduTempTableName)

    // Now refer to that temp table name in our Spark SQL statement
    val customerNameAgeDF = sqlContext
      .sql(s"""SELECT name, age FROM $kuduTempTableName WHERE age >= 30""")

    customerNameAgeDF.show()

    // Do Spark SQL, but using the API instead of in plain text

    // Do a groupBy here
    val custGroupedDF = sqlContext.read.options(kuduDFReadOptions).kudu
      .select("name", "age", "city")
      .where("age < 30")
      .groupBy("age")

    // Apply the max function as appropriate
    val custAggregated = custGroupedDF.agg(
      max("name").as("name_max"), max("city").as("city_max"))
    custAggregated.show()
  }
}
