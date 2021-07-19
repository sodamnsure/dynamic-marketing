package com.feelings.marketing.rule.demos;

import redis.clients.jedis.Jedis;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/19 10:38 上午
 */
public class RedisDemo {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("feelings", 6379);

        String status = jedis.ping();
        System.out.println(status);

        // 插入一条扁平数据
        jedis.set("key01", "value01");
        String key01 = jedis.get("key01");
        System.out.println(key01);
    }
}
