package main.scala
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext};

object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFilePath = "";
    println("------- My First SBT program.. Happy Coding -------")
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    println("**** Loaded Spark Configuration *** ")
    val sc = new SparkContext(conf)
    val text = sc.textFile("/Users/Akhil/input.txt",1)
    val words = text.flatMap(line => line.split(" "))
    println("................... Printing words ................")
    words.foreach(word => println(word))
    println("*********** PRINTING ENDS HERE ****************")
    val temp = words.map(word => (word,1)).reduceByKey(_ +_)
    temp.saveAsTextFile("/Users/Akhil/output.txt")
    println("************* Save Output to OUT file ***************")
  }
}
