package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 2:55 下午
 * @desc: 用户行为次序类条件查询服务实现（在 state 中查询）
 */
public class UserActionSeqQueryServiceStateImpl implements UserActionSeqQueryService {
    /**
     * 查询规则条件中的 行为序列条件
     * 会将查询到的最大匹配步骤， set回 ruleParam对象中
     * @param eventState flink中存储用户事件明细的state
     * @param ruleParam 规则参数对象
     * @return 返回成立与否
     */
    @Override
    public boolean queryActionSeq(ListState<LogBean> eventState, RuleParam ruleParam) {
        return false;
    }
}
