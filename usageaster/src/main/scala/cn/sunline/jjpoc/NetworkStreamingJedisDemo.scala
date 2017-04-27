package cn.sunline.jjpoc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by wanghg on 20/4/2017.
  */
object NetworkStreamingJedisDemo {

  def main(args: Array[String]): Unit = {
    // $ nc -lk 9999
    val conf = new SparkConf().setMaster("local[2]") setAppName ("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new TopN(List.empty[(String, Int)])
    ssc.sparkContext.register(acc)
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => {
      val jedis = new Jedis("localhost")
      rdd.foreach(acc.add)
      println("--------------------------------------------------" + this.getClass.getName)
      acc.value.foreach(_ match {
        case (word, count) =>
          val h = "words:count"
          val k = String.format("%s", word)
          val v = String.format("%d", count.asInstanceOf[Integer])
          jedis.hset(h, k, v)
          printf("jedis: (%s, %s) -> %s\n", k, v, h)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

