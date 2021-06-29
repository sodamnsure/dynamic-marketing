package com.feelings.marketing.rule.engine;

import com.feelings.marketing.rule.functions.*;
import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.ResultBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 3:51 下午
 * @desc: 静态规则引擎版本2主程序
 */
public class RuleEngineV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 添加kafka消费source
        DataStreamSource<String> logStream = env.addSource(SourceFunctions.getKafkaEventSource());

        // 将json格式的数据转成logBean对象
        SingleOutputStreamOperator<LogBean> beanStream = logStream.map(new JsonToBeanMapFunction());

        // 对数据按照用户deviceId分key
        KeyedStream<LogBean, String> keyed = beanStream.keyBy(new DeviceKeySelector());

        // 开始核心计算处理
        SingleOutputStreamOperator<ResultBean> resultStream = keyed.process(new RuleProcessFunctionV2());

        // 打印
        resultStream.print();

        env.execute();

    }
}
