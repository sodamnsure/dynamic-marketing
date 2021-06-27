package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.utils.ConnectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/25 4:48 下午
 * @desc: 行为序列类路径匹配查询service：clickhouse实现
 */
public class UserActionSeqQueryServiceClickHouseImpl implements UserActionSeqQueryService{

    Connection conn;

    public UserActionSeqQueryServiceClickHouseImpl() throws Exception {
        conn = ConnectionUtils.getClickHouseConnection();
    }

    @Override
    public boolean queryActionSeq(String deviceId, ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {
        return false;
    }
}
