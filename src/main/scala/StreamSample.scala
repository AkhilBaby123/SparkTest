package main.scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamSample {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming Test")
      val sc = new StreamingContext(conf,Seconds(10))
      val stream = sc.socketTextStream("localhost",9999)
      val words = stream.flatMap(line=>line.split(" "))
      words.map(word => (word,1)).reduceByKey(_+_).saveAsTextFiles("/Users/Akhil/StreamingOut/Out")
      sc.start()
      sc.awaitTermination()
  }
}
