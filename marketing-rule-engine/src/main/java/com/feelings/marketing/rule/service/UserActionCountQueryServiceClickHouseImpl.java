package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/21 8:42 下午
 */
public class UserActionCountQueryServiceClickHouseImpl implements UserActionCountQueryService{
    @Override
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
        for (RuleAtomicParam ruleAtomicParam : userActionCountParams) {
            //
        }

        return false;
    }
}
