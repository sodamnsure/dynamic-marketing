package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.utils.ClickHouseCountQuerySqlUtil;
import com.feelings.marketing.rule.utils.ConnectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/21 8:42 下午
 */
public class UserActionCountQueryServiceClickHouseImpl implements UserActionCountQueryService{
    Connection conn;

    public UserActionCountQueryServiceClickHouseImpl() throws Exception {
        conn = ConnectionUtils.getClickHouseConnection();
    }

    @Override
    public boolean queryActionCounts(String deviceId , ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
        // 遍历每一个原子条件
        for (RuleAtomicParam ruleAtomicParam : userActionCountParams) {
            // 对当前的原子条件拼接查询SQL
            String sql = ClickHouseCountQuerySqlUtil.getSql(deviceId, ruleAtomicParam);
            // 获取一个ClickHouse的jdbc链接
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            // deviceId、cnt
            while (resultSet.next()) {
                int realCnt = resultSet.getInt(2);
                ruleAtomicParam.setRealCounts(realCnt);
            }

            if (ruleAtomicParam.getRealCounts() < ruleAtomicParam.getThreshold()) return false;
        }

        // 如果到达这一句话，说明上面的每一个原子条件查询后都满足规则，那么返回最终结果true
        return true;
    }
}
