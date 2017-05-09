package cn.sunline.jjpoc

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by wanghg on 20/4/2017.
  */
object KafkaStreamingJedisPoc {

  def main(args: Array[String]): Unit = {
    // spark context settings
    val conf = new SparkConf().setMaster("local[2]") setAppName ("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new TopM(Double.MinValue, Double.MaxValue, 0.0, 0, List.empty[(String, Double)])
    ssc.sparkContext.register(acc)
    //
    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(pair => pair._2)
    //
    lines.foreachRDD(rdd => {
      val jedis = new Jedis("localhost")
      rdd.foreach(acc.add)
      println("--------------------------------------------------" + this.getClass.getName)
      print(acc)
      val (max, min, sum, cnt, avg, top5) = acc.value
      // --------------------------------------------------
      // clear agg
      jedis.del("poc:agg:name")
      jedis.del("poc:agg:value")
      // set max
      jedis.rpush("poc:agg:name", "max")
      jedis.rpush("poc:agg:value", String.format("%.2f", max.asInstanceOf[java.lang.Double]))
      // set min
      jedis.rpush("poc:agg:name", "min")
      jedis.rpush("poc:agg:value", String.format("%.2f", min.asInstanceOf[java.lang.Double]))
      // set sum
      jedis.rpush("poc:agg:name", "sum")
      jedis.rpush("poc:agg:value", String.format("%.2f", sum.asInstanceOf[java.lang.Double]))
      // set cnt
      jedis.rpush("poc:agg:name", "cnt")
      jedis.rpush("poc:agg:value", String.format("%d", cnt.asInstanceOf[java.lang.Integer]))
      // set sum
      jedis.rpush("poc:agg:name", "avg")
      jedis.rpush("poc:agg:value", String.format("%.2f", (sum/cnt).asInstanceOf[java.lang.Double]))
      // --------------------------------------------------
      // clear top5
      jedis.del("poc:top5:name")
      jedis.del("poc:top5:value")
      // set top5
      top5.foreach(pair => {
        val (k, v) = pair
        jedis.rpush("poc:top5:name", k)
        jedis.rpush("poc:top5:value", String.format("%.2f", v.asInstanceOf[java.lang.Double]))
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

