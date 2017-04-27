package cn.sunline.jjpoc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ListBuffer

/**
  * Created by wanghg on 27/4/2017.
  */
class TopM(mx: Double, mi: Double, l: List[(String, Double)]) extends AccumulatorV2[String, (Double, Double, List[(String, Double)])] {

  private[this] var max = mx
  private[this] var min = mi
  private[this] var list = l

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[String, (Double, Double, List[(String, Double)])] = new TopM(max, min, list)

  override def reset(): Unit = {
    max = Double.MinValue
    min = Double.MaxValue
    list = List.empty[(String, Double)]
  }

  override def merge(other: AccumulatorV2[String, (Double, Double, List[(String, Double)])]): Unit = {
    val (mx, mi, l) = other.value
    max = if (max > mx) max else mx
    min = if (min < mi) min else mi
    for (v <- l) add(v._1 + "," + v._2)
  }

  override def add(v: String): Unit = {
    val idx = v.indexOf(',')
    val v1 = v.substring(0, idx).trim()
    val v2 = v.substring(idx + 1).trim()
    val dbl = v2.toDouble
    max = if (dbl > max) dbl else max
    min = if (dbl < min) dbl else min
    list = add(list, v1, dbl, ListBuffer.empty[(String, Double)])
  }

  override def value: (Double, Double, List[(String, Double)]) = (max, min, list)

  private def add(list: List[(String, Double)], key: String, value: Double, acc: ListBuffer[(String, Double)]): List[(String, Double)] = {
    list match {
      case Nil =>
        acc.+=((key, value))
        acc.toList.take(5)
      case (k, v) :: rest if v >= value =>
        acc.+=((k, v))
        add(rest, key, value, acc)
      case (k, v) :: rest if v < value =>
        acc.+=((key, value))
        acc.++=(list)
        acc.toList.take(5)
    }
  }

  override def toString(): String = {
    val sb = new StringBuilder
    sb.append(String.format("max: %.2f\n", max.asInstanceOf[java.lang.Double]))
    sb.append(String.format("min: %.2f\n", min.asInstanceOf[java.lang.Double]))
    sb.append(String.format("top5:\n"))
    list.foreach(pair => {
      val (k, v) = pair
      sb.append(String.format("\t%s: %.2f\n", k, v.asInstanceOf[java.lang.Double]))
    })
    sb.toString
  }

}

