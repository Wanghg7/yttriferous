package cn.sunline.jjpoc

import scala.util.Random

/**
  * Created by wanghg on 27/4/2017.
  */
object GenFile {

  def main(args: Array[String]): Unit = {
    var count = args(0).toInt
    while (count > 0) {
      printf("ID_%06x,%d.%02d\n", Random.nextInt(0x1000000), Random.nextInt(0x100), Random.nextInt(0x10))
      count = count - 1
    }
  }

}
