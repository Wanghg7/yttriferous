package cn.sunline.jjpoc

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanghg on 27/4/2017.
  */
object KafkaStreamingDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]") setAppName ("KafkaDemo")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new TopN(List.empty[(String, Int)])
    ssc.sparkContext.register(acc)
    //
    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(pair => pair._2)
    //
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => {
      rdd.foreach(acc.add)
      println("--------------------------------------------------" + this.getClass.getName)
      acc.value.sortWith(_._2 > _._2) foreach (println)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
