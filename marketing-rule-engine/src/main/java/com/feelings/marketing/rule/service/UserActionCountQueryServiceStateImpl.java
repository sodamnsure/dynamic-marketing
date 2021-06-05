package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:17 上午
 * @desc: 用户行为次数类条件查询服务实现：在flink的state中统计行为次数
 */
public class UserActionCountQueryServiceStateImpl implements UserActionCountQueryService {
    /**
     * 查询规则参数对象中，要求的用户行为次数类条件是否满足
     * 同时将查询到的真实次数 set 回规则参数对象中
     * @param eventState 用户事件明确存储state
     * @param ruleParam 规则整体参数对象
     * @return 条件是否满足
     */
    @Override
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) {
        ruleParam.getUserActionParams();
        return false;
    }
}
