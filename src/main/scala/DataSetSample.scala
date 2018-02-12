package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataSetSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataSetSample").getOrCreate()
    val configMap = spark.conf.getAll
//    println("*********** Printing Spark Configurations ************")
//    configMap.foreach(conf => println(conf))
    /*val numDS = spark.range(10, 100, 10)
    numDS.orderBy(desc("id")).show(10)
    println("********* DESCRIBE *********")
    numDS.describe("id").show()
    val schema = StructType(StructField("name", StringType, false) :: StructField("age", IntegerType, false) :: Nil)
    val frndsDS = spark.createDataFrame(List(("Akhil", 30), ("Nithin", 30)))
    val frndsDSSchema = frndsDS.withColumnRenamed("_1", "name").withColumnRenamed("_2", "age")
    frndsDSSchema.show()*/

    println("**************** <<<<<<< Reading data from JSON File >>>>>>>> *************")

    val jsonData = spark.read.json("/Users/Akhil/json.json")
    jsonData.describe().show()
    jsonData.createTempView("frndsTable")
    val temp = spark.sql("Describe frndsTable")
    val records = spark.sql("Select * from frndsTable where name='akhil'")
    records.show()

  }
}
