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
 * V2版本存在的问题:
 *  查询策略：
 *      1. 近期的条件在state中查询
 *      2. 远期的条件在clickhouse中查询
 *  存在的问题：
 *      1. 近期的查询没有问题
 *      2. 远期有问题：
 *          比如一个条件的查询时间跨度是跨分界点的，2.0直接就交给clickhouse查询
 *              - clickhouse中的数据是批次入库的，有可能在查询时，最近的数据还没入库
 *              - 全部交给clickhouse查询，给clickhouse带来的查询压力比较大
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
