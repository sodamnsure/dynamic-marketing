package com.feelings.marketing.rule.functions;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.ResultBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.service.*;
import com.feelings.marketing.rule.utils.RuleSimulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/16 5:51 下午
 * @desc: 规则核心处理函数
 */
public class RuleProcessFunctionV2 extends KeyedProcessFunction<String, LogBean, ResultBean> {
    private UserProfileQueryService userProfileQueryService;
    private UserActionCountQueryService userActionCountQueryStateService;
    private UserActionSeqQueryService userActionSeqQueryStateService;


    private UserActionCountQueryService userActionCountQueryClickHouseService;
    private UserActionSeqQueryService userActionSeqQueryClickHouseService;


    RuleParam ruleParam;
    ListState<LogBean> eventState;

    @Override
    public void open(Configuration parameters) throws Exception {
        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        /**
         * 构造底层的核心State查询服务
         */
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl();
        userActionSeqQueryStateService = new UserActionSeqQueryServiceStateImpl();

        /**
         * 构造底层的核心ClickHouse查询服务
         */
        userActionCountQueryClickHouseService = new UserActionCountQueryServiceClickHouseImpl();
        userActionSeqQueryClickHouseService = new UserActionSeqQueryServiceClickHouseImpl();

        /**
         * 获取规则参数
         * TODO 规则的获取，现在是通过模拟器生成
         * TODO 后期需要改造成从外部获取
         */
        ruleParam = RuleSimulator.getRuleParam();

        /**
         * 准备一个存储明细事件的state
         * 控制state的ttl周期为最近2小时
         */
        ListStateDescriptor<LogBean> desc = new ListStateDescriptor<>("eventState", LogBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).updateTtlOnCreateAndWrite().build();
        desc.enableTimeToLive(ttlConfig);
        eventState = getRuntimeContext().getListState(desc);
    }

    /**
     * 规则计算核心方法，来一个事件调用一次
     *
     * @param logBean
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(LogBean logBean, Context context, Collector<ResultBean> collector) throws Exception {
        // 将收到的事件放入历史明细state存储中
        // 超过2小时的logBean会被自动清除（前面设置了ttl存活时长）
        eventState.add(logBean);
        // 计算当前时间的前两小时时间戳
        long splitPoint = System.currentTimeMillis() - 2 * 60 * 60 * 1000;


        /**
         * 主逻辑，进行规则触发和计算
         */
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            System.out.println("规则计算被触发： " + logBean.getDeviceId() + ", " + logBean.getEventId());

            // 查询画像条件
            boolean profileIfMatch = userProfileQueryService.judgeProfileCondition(logBean.getEventId(), ruleParam);
            if (!profileIfMatch) return;

            /**
             * 查询事件次数类条件是否满足
             * 这里要考虑，条件的事件跨度问题
             * 如果条件的时间跨度在2小时内，那么，就把这些条件交给stateService去计算
             * 如果时间的时间跨度在2小时之前，那么，就把这些条件交给ClickHouseService去计算
             */
            // 遍历规则中的次数类条件，按照时间跨度，分成两组
            List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
            ArrayList<RuleAtomicParam> forwardRangeParams = new ArrayList<>();  // 存远跨度的条件的list
            ArrayList<RuleAtomicParam> nearRangeParams = new ArrayList<>(); // 存近跨度的条件list

            for (RuleAtomicParam userActionCountParam : userActionCountParams) {
                if (userActionCountParam.getRangeStart() < splitPoint) {
                    // 如果条件起始时间小于2小时分界点，放入远期查询list
                    forwardRangeParams.add(userActionCountParam);
                } else {
                    // 否则，放入近期查询list
                    nearRangeParams.add(userActionCountParam);
                }
            }

            // 查询state中行为次数条件
            if (nearRangeParams.size() > 0) {
                // 将规则总参数对象中的"次数条件"覆盖成：近期条件组
                ruleParam.setUserActionCountParams(nearRangeParams);
                // 交给stateService，对这一组条件进行计算
                boolean countMatch = userActionCountQueryStateService.queryActionCounts("", eventState, ruleParam);
                if (!countMatch) return;
            }


            // 如果在state中查询的部分条件满足，则继续在ClickHouse中查询各个远期条件
            if (forwardRangeParams.size() > 0) {
                // 将规则总参数中对象中的"次数类条件"覆盖成：远期条件组
                ruleParam.setUserActionCountParams(forwardRangeParams);
                boolean b = userActionCountQueryClickHouseService.queryActionCounts(logBean.getDeviceId(), null, ruleParam);
                if (!b) return;
            }


            /**
             * 序列类条件的查询
             * 本项目对序列类条件进行了简化
             * 一个规则中，只存在一个"序列模式"
             * 它的条件时间跨度，设置在了这个"序列模式"中的每一个原子条件中，且都一致
             */
            // 取出规则中的序列模式
            List<RuleAtomicParam> userActionSeqParams = ruleParam.getUserActionSeqParams();
            // 如果序列模型中的起始时间小于2小时分界点，则交给ClickHouse服务模块去处理
            if (userActionSeqParams != null && userActionSeqParams.size() > 0 && userActionSeqParams.get(0).getRangeStart() < splitPoint) {
                boolean b = userActionSeqQueryClickHouseService.queryActionSeq(logBean.getDeviceId(), null, ruleParam);
                if (!b) return;
            }

            // 如果序列模型中的起始时间大于等于2小时分界点，则交给ClickHouse服务模块去处理
            if (userActionSeqParams.size() > 0 && userActionSeqParams.get(0).getRangeStart() >= splitPoint) {
                boolean seqMatch = userActionSeqQueryStateService.queryActionSeq("", eventState, ruleParam);
                if (!seqMatch) return;
            }


            // 输出一个规则匹配成功的结果
            ResultBean resultBean = new ResultBean();
            resultBean.setTimeStamp(logBean.getTimeStamp());
            resultBean.setRuleId(ruleParam.getRuleId());
            resultBean.setDeviceId(logBean.getDeviceId());

            // 返回
            collector.collect(resultBean);

            // 测试提交数据

        }

    }
}
