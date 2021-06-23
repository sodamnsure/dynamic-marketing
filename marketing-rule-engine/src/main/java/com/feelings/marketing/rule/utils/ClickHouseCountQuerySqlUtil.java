package com.feelings.marketing.rule.utils;

import com.feelings.marketing.rule.pojo.RuleAtomicParam;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @Author: sodamnsure
 * @Date: 2021/6/22 2:12 下午
 * @desc: 行为次数类条件查询SQL拼接工具
 */
public class ClickHouseCountQuerySqlUtil {
    public static String getSql(String deviceId, RuleAtomicParam atomicParam) {
        // 设置select条件
        String select = "select " +
                "deviceId, count(*) as cnt " +
                "from event_detail " +
                "where deviceId = '" + deviceId + "'" +
                " and " +
                "eventId = '" + atomicParam.getEventId() + "'" +
                "and timeStamp >= "+ atomicParam.getRangeStart() +
                " and timeStamp <=" + atomicParam.getRangeEnd();

        String groupBy = " group by deviceId";

        HashMap<String, String> properties = atomicParam.getProperties();
        Set<Map.Entry<String, String>> entries = properties.entrySet();
        StringBuffer stringBuffer = new StringBuffer();

        for (Map.Entry<String, String> entry : entries) {
            stringBuffer.append(" and properties['" + entry.getKey() + "'] = '" + entry.getValue() + "'");
        }

        return select + stringBuffer.toString() + groupBy;
    }

    public static void main(String[] args) {
        RuleAtomicParam ruleAtomicParam = new RuleAtomicParam();
        ruleAtomicParam.setEventId("R");
        HashMap<String, String> props = new HashMap<>();
        props.put("p1", "v2");
        props.put("p3", "v4");
        props.put("p7", "v5");
        ruleAtomicParam.setProperties(props);
        ruleAtomicParam.setRangeStart(1624259490176L);
        ruleAtomicParam.setRangeEnd(1624259494176L);
        String sql = getSql("000001", ruleAtomicParam);
        System.out.printf(sql);
    }
}
