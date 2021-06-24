package com.feelings.marketing.rule.functions;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.ResultBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.service.*;
import com.feelings.marketing.rule.utils.RuleSimulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/16 5:51 下午
 * @desc: 规则核心处理函数
 */
public class RuleProcessFunction extends KeyedProcessFunction<String, LogBean, ResultBean> {
    private UserProfileQueryService userProfileQueryService;
    private UserActionCountQueryService userActionCountQueryService;
    private UserActionSeqQueryService userActionSeqQueryService;

    RuleParam ruleParam;
    ListState<LogBean> eventState;

    @Override
    public void open(Configuration parameters) throws Exception {
        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();
        userActionCountQueryService = new UserActionCountQueryServiceStateImpl();
        userActionSeqQueryService = new UserActionSeqQueryServiceStateImpl();

        ruleParam = RuleSimulator.getRuleParam();

        ListStateDescriptor<LogBean> desc = new ListStateDescriptor<>("eventState", LogBean.class);
        eventState = getRuntimeContext().getListState(desc);
    }

    @Override
    public void processElement(LogBean logBean, Context context, Collector<ResultBean> collector) throws Exception {
        // 将收到的事件放入历史明细state存储中
        eventState.add(logBean);

        // 判断是否满足触发条件
        if (ruleParam.getTriggerParam().getEventId().equals(logBean.getEventId())) {
            // 查询画像条件
            boolean profileMatch = userProfileQueryService.judgeProfileCondition(logBean.getEventId(), ruleParam);
            if (!profileMatch) return;

            // 查询行为次数条件
            boolean countMatch = userActionCountQueryService.queryActionCounts("",eventState, ruleParam);
            if (!countMatch) return;

            // 查询行为序列条件
            boolean seqMatch = userActionSeqQueryService.queryActionSeq(eventState, ruleParam);
            if (!seqMatch) return;

            // 输出一个规则匹配成功的结果
            ResultBean resultBean = new ResultBean();
            resultBean.setTimeStamp(logBean.getTimeStamp());
            resultBean.setRuleId(ruleParam.getRuleId());
            resultBean.setDeviceId(logBean.getDeviceId());

            // 返回
            collector.collect(resultBean);

        }

    }
}
