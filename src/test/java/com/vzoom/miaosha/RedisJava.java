package com.vzoom.miaosha;
import redis.clients.jedis.Jedis;

public class RedisJava {

    public static void main(String[] args) {

       //连接本地的 Redis 服务
      Jedis jedis = new Jedis("192.168.11.123");
//      Jedis jedis = new Jedis("192.168.61.14");
      jedis.auth("root");
      jedis.select(0);
      //查看服务是否运行
      System.out.println("Server is running: "+jedis.ping());
      System.out.println(jedis.get("name"));;
    }
}