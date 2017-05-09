package cn.sunline.jjpoc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanghg on 20/4/2017.
  */
object NetworkStreamingPoc {

  def main(args: Array[String]): Unit = {
    // $ nc -lk 9999
    val conf = new SparkConf().setMaster("local[2]") setAppName ("NetworkDemo")
    val ssc = new StreamingContext(conf, Seconds(5))
    val acc = new TopM(Double.MinValue, Double.MaxValue, 0.0, 0, List.empty[(String, Double)])
    ssc.sparkContext.register(acc)
    val lines = ssc.socketTextStream("localhost", 9999)
    lines.foreachRDD(rdd => {
      rdd.foreach(acc.add)
      println("--------------------------------------------------" + this.getClass.getName)
      print(acc)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

