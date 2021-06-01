package com.feelings.marketing.rule.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.ResultBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Properties;

/**
 * @Author: sodamnsure
 * @Date: 2021/5/28 2:27 下午
 * @desc: 实时运营系统版本1.0
 * <p>
 * 规则：
 * 触发条件：E事件
 * 画像属性条件：k3=v3, k100=v80, k230=v360
 * 行为属性条件：U(p1=v3, p2=v2) >= 3次 且 G(p6=v8, p4=v5, p1=v2) >= 1次
 * 行为次序条件：依次做过--> W(p1=v4) --> R(p2 = v3) --> F
 */
public class RuleEngineV1 {
    public static void main(String[] args) throws Exception {
        // 创建Environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 添加kafka数据源
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "feelings:9092");
        prop.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("ActionLog", new SimpleStringSchema(), prop);

        // 将数据源添加到Environment
        DataStreamSource<String> logStream = env.addSource(kafkaSource);

        // 由于数据是json格式，需要将数据转成bean对象
        ObjectMapper mapper = new ObjectMapper();
        SingleOutputStreamOperator<LogBean> beanStream = logStream.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String line) throws Exception {
                return mapper.readValue(line, LogBean.class);
            }
        });

        // 对数据按照用户keyBy, keyBy里面传入方法，Java里面是不能传递函数的，但是函数可以放在匿名内部类里面
        KeyedStream<LogBean, String> keyed = beanStream.keyBy(new KeySelector<LogBean, String>() {
            @Override
            public String getKey(LogBean bean) throws Exception {
                // 输入一个对象，需要返回这条数据的key是谁
                return bean.getDeviceId();
            }
        });

        // 在这个keyBy数据流上做规则判断
        SingleOutputStreamOperator<ResultBean> resultStream = keyed.process(new KeyedProcessFunction<String, LogBean, ResultBean>() {
            Connection conn;
            Table table;
            ListState<LogBean> eventState;

            // open方法Task只调用一次，适合创建数据链接
            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建HBASE配置
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("hbase.zookeeper.quorum", "feelings:2181");
                // 创建HBASE链接
                conn = ConnectionFactory.createConnection(conf);
                // 创建链接后要拿到表
                table = conn.getTable(TableName.valueOf("user_profile"));

                // 定义一个list结构的state
                ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("event_state", LogBean.class);
                eventState = getRuntimeContext().getListState(eventStateDesc);
            }

            @Override
            public void processElement(LogBean bean, Context context, Collector<ResultBean> collector) throws Exception {
                // 先将接收到的这条数据存储到state中
                eventState.add(bean);

                // 判断当前用户的行为是否满足规则中的触发条件
                if ("E".equals(bean.getEventId())) {
                    // 判断画像属性条件：k3=v3, k100=v80, k230=v360; 查询HBASE
                    // 构造查询条件
                    Get get = new Get(Bytes.toBytes(bean.getDeviceId()));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k3"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k100"));
                    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("k230"));

                    // 传入查询条件并查询
                    Result result = table.get(get);
                    String k3Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k3")));
                    String k100Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k100")));
                    String k230Value = new String(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("k230")));

                    if ("v3".equals(k3Value) && "v80".equals(k100Value) && "v360".equals(k230Value)) {
                        // *  行为属性条件：U(p1=v3, p2=v2) >= 3次 且 G(p6=v8, p4=v5, p1=v2) >= 1次

                    }


                    // *  行为次序条件：依次做过--> W(p1=v4) --> R(p2 = v3) --> F

                }
            }
        });


        // 打印测试
        resultStream.print();

        env.execute();

    }
}
