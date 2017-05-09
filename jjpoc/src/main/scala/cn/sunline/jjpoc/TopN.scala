package cn.sunline.jjpoc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ListBuffer

/**
  * Created by wanghg on 27/4/2017.
  */
class TopN(l: List[(String, Int)]) extends AccumulatorV2[(String, Int), List[(String, Int)]] {

  private[this] var list = l

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[(String, Int), List[(String, Int)]] = new TopN(list)

  override def reset(): Unit = list = List.empty[(String, Int)]

  override def merge(other: AccumulatorV2[(String, Int), List[(String, Int)]]): Unit = {
    val l = other.value
    for (v <- l) add(v)
  }

  override def add(v: (String, Int)): Unit = {
    list = add(list, v, ListBuffer.empty[(String, Int)])
  }

  override def value: List[(String, Int)] = list

  private def add(list: List[(String, Int)], v: (String, Int), acc: ListBuffer[(String, Int)]): List[(String, Int)] = {
    val (newWord, deltaCount) = v
    list match {
      case Nil => acc.+=(v).toList
      case (word, count) :: xs if word == newWord => acc.+=((word, deltaCount + count)).++=(xs).toList
      case (word, count) :: xs if word != newWord => add(xs, v, acc += ((word, count)))
    }
  }
}
