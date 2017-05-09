package cn.sunline.jjpoc

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object App {

  def main(args: Array[String]): Unit = {
//    NetworkStreamingJedisDemo.run()
  }

  def kafkaWordCount(): Unit = {
    val conf = new SparkConf().setMaster("local[2]") setAppName ("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new TopN(List.empty[(String, Int)])
    ssc.sparkContext.register(acc)
    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(pair => pair._2)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => {
      rdd.foreach(acc.add)
      println("--------------------------------------------------")
//      printAcc(acc)
    })
    ssc.start()
    ssc.awaitTermination()
  }
/*
  def accToView(conf: SparkConf, acc: AccumulatorV2[(String, Int), List[(String, Int)]], viewName: String, colNames: String*): Unit = {
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    acc.value.toDF(colNames: _*).createGlobalTempView(viewName)
  }

  def printAcc(acc: AccumulatorV2[(String, Int), List[(String, Int)]]): Unit = {
    acc.value.sortWith(_._2 > _._2).foreach(println)
  }

  def printView(conf: SparkConf, viewName: String, colNames: String*): Unit = {
    val spark = SparkSession.builder.config(conf).getOrCreate()
    var rs = spark.sql("select " + colNames.mkString(", ") + " from " + viewName)
    rs.show
  }
*/
}



