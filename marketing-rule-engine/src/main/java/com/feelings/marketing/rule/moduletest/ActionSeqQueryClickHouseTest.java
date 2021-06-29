package com.feelings.marketing.rule.moduletest;

import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.service.UserActionSeqQueryServiceClickHouseImpl;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/28 5:18 下午
 * @desc: 用户行为路径类匹配查询测试
 */
public class ActionSeqQueryClickHouseTest {
    public static void main(String[] args) throws Exception {
        // 查询SQL
        String sql = "select deviceId,\n" +
                "       sequenceMatch('.*(?1).*(?2).*(?3)')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'Z' and properties['p3'] = 'v8',\n" +
                "                     eventId = 'I' and properties['p2'] = 'v6',\n" +
                "                     eventId = 'A' and properties['p8'] = 'v7'\n" +
                "           ) as is_match3,\n" +
                "       sequenceMatch('.*(?1).*(?2).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'Z' and properties['p3'] = 'v8',\n" +
                "                     eventId = 'I' and properties['p2'] = 'v6',\n" +
                "                     eventId = 'A' and properties['p8'] = 'v7'\n" +
                "           ) as is_match2,\n" +
                "       sequenceMatch('.*(?1).*')(\n" +
                "                     toDateTime(`timeStamp`),\n" +
                "                     eventId = 'Z' and properties['p3'] = 'v8',\n" +
                "                     eventId = 'I' and properties['p2'] = 'v6',\n" +
                "                     eventId = 'A' and properties['p8'] = 'v7'\n" +
                "           ) as is_match1\n" +
                "from default.event_detail\n" +
                "where deviceId = '008788'\n" +
                "  and timeStamp >= 0\n" +
                "  and timeStamp < 5928492919183\n" +
                "  and (\n" +
                "        (eventId = 'Z' and properties['p3'] = 'v8')\n" +
                "        or (eventId = 'I' and properties['p2'] = 'v6')\n" +
                "        or (eventId = 'A' and properties['p8'] = 'v7')\n" +
                "    )\n" +
                "group by deviceId;";


        // 构造序列条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("Z");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p3", "v8");
        param1.setProperties(paramProps1);
        param1.setRangeStart(0L);
        param1.setRangeEnd(Long.MAX_VALUE);


        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("I");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2", "v6");
        param2.setProperties(paramProps2);
        param2.setRangeStart(0L);
        param2.setRangeEnd(Long.MAX_VALUE);


        RuleAtomicParam param3 = new RuleAtomicParam();
        param3.setEventId("A");
        HashMap<String, String> paramProps3 = new HashMap<>();
        paramProps3.put("p8", "v7");
        param3.setProperties(paramProps3);
        param3.setRangeStart(0L);
        param3.setRangeEnd(Long.MAX_VALUE);


        ArrayList<RuleAtomicParam> ruleAtomicParams = new ArrayList<>();
        ruleAtomicParams.add(param1);
        ruleAtomicParams.add(param2);
        ruleAtomicParams.add(param3);

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionSeqParams(ruleAtomicParams);
        ruleParam.setActionSeqQuerySql(sql);

        UserActionSeqQueryServiceClickHouseImpl serviceClickHouse = new UserActionSeqQueryServiceClickHouseImpl();
        boolean b = serviceClickHouse.queryActionSeq("008788", null, ruleParam);
        System.out.println(b + "," + ruleParam.getUserActionSeqQueriedMaxStep());
    }
}
