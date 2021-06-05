package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:16 上午
 * @desc: 用户行为次数类条件查询服务接口
 */
public interface UserActionCountQueryService {

    boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam);
}
