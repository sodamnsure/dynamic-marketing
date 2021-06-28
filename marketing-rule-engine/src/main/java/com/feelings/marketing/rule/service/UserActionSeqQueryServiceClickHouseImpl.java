package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.utils.ConnectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/25 4:48 下午
 * @desc: 行为序列类路径匹配查询service：clickhouse实现
 */
public class UserActionSeqQueryServiceClickHouseImpl implements UserActionSeqQueryService{

    private Connection conn;

    public UserActionSeqQueryServiceClickHouseImpl() throws Exception {
        conn = ConnectionUtils.getClickHouseConnection();
    }

    @Override
    public boolean queryActionSeq(String deviceId, ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {
        // 获取规则中，路径模式的总步骤数
        int totalStep = ruleParam.getUserActionSeqParams().size();
        // 取出查询SQL
        String sql = ruleParam.getActionSeqQuerySql();
        Statement statement = conn.createStatement();
        // 执行查询SQL
        ResultSet resultSet = statement.executeQuery(sql);
        // 从返回结果中进行条件判断
        int i = 2;
        int maxStep = 0;
        while (resultSet.next()) {
            for (;i < totalStep + 2; i++) {
                maxStep += resultSet.getInt(i);
            }
        }

        // 返回最大步骤号
        ruleParam.setUserActionSeqQueriedMaxStep(maxStep);
        return maxStep==totalStep;
    }
}
