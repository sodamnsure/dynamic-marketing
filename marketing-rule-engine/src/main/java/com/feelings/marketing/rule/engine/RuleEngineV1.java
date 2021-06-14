package com.feelings.marketing.rule.engine;

import com.feelings.marketing.rule.functions.SourceFunctions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 3:51 下午
 * @desc: 静态规则引擎版本1主程序
 */
public class RuleEngineV1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 添加kafka消费source
        DataStreamSource<String> logStream = env.addSource(SourceFunctions.getKafkaEventSource());
        // 将json格式的数据转成logBean对象


    }
}
