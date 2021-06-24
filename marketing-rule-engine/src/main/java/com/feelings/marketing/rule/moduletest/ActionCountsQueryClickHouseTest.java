package com.feelings.marketing.rule.moduletest;

import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.service.UserActionCountQueryServiceClickHouseImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/24 4:31 下午
 */
public class ActionCountsQueryClickHouseTest {
    public static void main(String[] args) throws Exception {
        UserActionCountQueryServiceClickHouseImpl impl = new UserActionCountQueryServiceClickHouseImpl();

        // 构造2个规则原子条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("E");
        param1.setRangeStart(0L);
        param1.setRangeEnd(Long.MAX_VALUE);
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1", "v9");
        param1.setProperties(paramProps1);
        param1.setThreshold(1);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("N");
        param2.setRangeStart(0L);
        param2.setRangeEnd(Long.MAX_VALUE);
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2", "v1");
        param2.setProperties(paramProps2);
        param2.setThreshold(1);

        ArrayList<RuleAtomicParam> ruleAtomicParams = new ArrayList<>();
        ruleAtomicParams.add(param1);
        ruleAtomicParams.add(param2);

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserActionCountParams(ruleAtomicParams);

        boolean b = impl.queryActionCounts("004762", null, ruleParam);

        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            System.out.println(userActionCountParam.getThreshold() + ", " + userActionCountParam.getRealCounts());
        }


    }
}
