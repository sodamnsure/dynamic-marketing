package com.feelings.marketing.rule.functions;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 5:28 下午
 */
public class SourceFunctions {
    public static FlinkKafkaConsumer<String> getKafkaEventSource() {
        // 添加kafka数据源
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "feelings:9092");
        props.setProperty("auto.offset.reset", "latest");

        return new FlinkKafkaConsumer<>("ActionLog", new SimpleStringSchema(), props);

    }

}
