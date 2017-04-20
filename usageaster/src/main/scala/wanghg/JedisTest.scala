package wanghg

import redis.clients.jedis.Jedis

/**
  * Created by wanghg on 20/4/2017.
  */
object JedisTest {

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("localhost")
    val value = jedis.get("wanghg")
    println(value)
  }

}

