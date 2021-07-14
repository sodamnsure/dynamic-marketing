package com.feelings.marketing.rule.utils;

import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/16 5:58 下午
 * @desc: 规则模拟器
 */
public class RuleSimulator {
    public static RuleParam getRuleParam() {
        RuleParam ruleParam = new RuleParam();
        ruleParam.setRuleId("test_rule_1");

        // 触发条件
        RuleAtomicParam trigger = new RuleAtomicParam();
        trigger.setEventId("E");
        ruleParam.setTriggerParam(trigger);

        // 画像条件
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("k12", "v117");
        userProfileParams.put("k22", "v978");
        ruleParam.setUserProfileParams(userProfileParams);

        // 行为次数条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("B");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1", "v1");
        param1.setProperties(paramProps1);
        param1.setThreshold(2);
        param1.setRangeStart(0L);
        param1.setRangeEnd(Long.MAX_VALUE);
        String sql1 = "select deviceId,\n" +
                "       count(1) as cnt\n" +
                "from default.event_detail\n" +
                "where deviceId = '${deviceId}' and eventId = 'B' and properties['p1'] = 'v1'\n" +
                "and timeStamp between 0 and 5928492919183\n" +
                "group by deviceId;";
        param1.setCountQuerySql(sql1);


        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("D");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2", "v3");
        param2.setProperties(paramProps2);
        param2.setThreshold(2);
        param2.setRangeStart(1625279800000L);
        param2.setRangeEnd(Long.MAX_VALUE);
        String sql2 = "select deviceId,\n" +
                "       count(1) as cnt\n" +
                "from default.event_detail\n" +
                "where deviceId = '${deviceId}' and eventId = 'D' and properties['p2'] = 'v3'\n" +
                "and timeStamp between 1625279800000 and 5928492919183\n" +
                "group by deviceId;";
        param2.setCountQuerySql(sql2);

        ArrayList<RuleAtomicParam> countParams = new ArrayList<>();
        countParams.add(param1);
        countParams.add(param2);


        ruleParam.setUserActionCountParams(countParams);


        // 行为序列条件
        RuleAtomicParam seqParam1 = new RuleAtomicParam();
        seqParam1.setEventId("A");
        HashMap<String, String> seqProps1 = new HashMap<>();
        seqProps1.put("p1", "v1");
        seqParam1.setProperties(seqProps1);
        seqParam1.setRangeStart(0L);
        seqParam1.setRangeEnd(Long.MAX_VALUE);


        RuleAtomicParam seqParam2 = new RuleAtomicParam();
        seqParam2.setEventId("C");
        HashMap<String, String> seqProps2 = new HashMap<>();
        seqProps2.put("p2", "v3");
        seqParam2.setProperties(seqProps2);
        seqParam2.setRangeStart(0L);
        seqParam2.setRangeEnd(Long.MAX_VALUE);


        ArrayList<RuleAtomicParam> seqParams = new ArrayList<>();
        seqParams.add(seqParam1);
        seqParams.add(seqParam2);

        ruleParam.setUserActionSeqParams(seqParams);
        String sql = "select deviceId,\n" +
                "       sequenceMatch('.*(?1).*(?2).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'A' and properties['p1'] = 'v1',\n" +
                "                     eventId = 'C' and properties['p2'] = 'v3'\n" +
                "           ) as is_match2,\n" +
                "       sequenceMatch('.*(?1).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'A' and properties['p1'] = 'v1',\n" +
                "                     eventId = 'C' and properties['p2'] = 'v3'\n" +
                "           ) as is_match1\n" +
                "from default.event_detail\n" +
                "where deviceId = '${deviceId}'\n" +
                "  and timeStamp >= 0\n" +
                "  and timeStamp < 5928492919183\n" +
                "  and (\n" +
                "        (eventId = 'A' and properties['p1'] = 'v1')\n" +
                "        or (eventId = 'C' and properties['p2'] = 'v3')\n" +
                "    )\n" +
                "group by deviceId";
        ruleParam.setActionSeqQuerySql(sql);

        return ruleParam;

    }
}
