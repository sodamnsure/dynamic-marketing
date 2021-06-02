package com.feelings.marketing.rule.benchmark;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/2 2:41 下午
 * @desc: HBASE查询压力测试
 */
public class HbaseGetTest {
    public static void main(String[] args) throws IOException {
        // 创建HBASE配置
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "feelings:2181");
        // 创建HBASE链接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 创建链接后要拿到表
        Table table = conn.getTable(TableName.valueOf("user_profile"));

        long s = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            Get get = new Get(StringUtils.leftPad(RandomUtils.nextInt(1, 900000) + "", 6, "0").getBytes());
            int i1 = RandomUtils.nextInt(1, 100);
            int i2 = RandomUtils.nextInt(1, 100);
            int i3 = RandomUtils.nextInt(1, 100);
            get.addColumn("f".getBytes(), Bytes.toBytes("k" + i1));
            get.addColumn("f".getBytes(), Bytes.toBytes("k" + i2));
            get.addColumn("f".getBytes(), Bytes.toBytes("k" + i3));

            Result result = table.get(get);

            // 查询数据
            byte[] v1 = result.getValue("f".getBytes(), Bytes.toBytes("k" + i1));
            byte[] v2 = result.getValue("f".getBytes(), Bytes.toBytes("k" + i2));
            byte[] v3 = result.getValue("f".getBytes(), Bytes.toBytes("k" + i2));
        }

        long e = System.currentTimeMillis();
        System.out.println(e - s);

        // 关闭链接
        conn.close();
    }
}
