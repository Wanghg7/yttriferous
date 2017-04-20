package wanghg

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanghg on 20/4/2017.
  */
object NetworkWordCount {

  def run(): Unit = {
    // $ nc -lk 9999
    val conf = new SparkConf().setMaster("local[2]") setAppName ("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new Top3(List.empty[(String, Int)])
    ssc.sparkContext.register(acc)
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => {
      rdd.foreach(acc.add)
      println("--------------------------------------------------")
      acc.value.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

