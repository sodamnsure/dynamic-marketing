package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 2:54 下午
 * @desc: 用户行为次序条件查询服务接口
 */
public interface UserActionSeqQueryService {

    boolean queryActionSeq(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception;
}
