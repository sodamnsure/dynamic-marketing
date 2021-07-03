package com.feelings.marketing.connector.jdbc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/18 4:50 下午
 * @desc: 清洗电站数据
 */
public class CleanStation {
    public static void main(String[] args) throws Exception {
        // 创建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定并行度
        env.setParallelism(2);
        // 添加MySQL source




        env.execute();

    }
}
