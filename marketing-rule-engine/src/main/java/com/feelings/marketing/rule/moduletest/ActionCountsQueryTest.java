package com.feelings.marketing.rule.moduletest;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.service.UserActionCountQueryServiceStateImpl;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/8 6:11 下午
 * @desc: 行为次数查询服务功能测试
 */
public class ActionCountsQueryTest {
    public static void main(String[] args) {
        UserActionCountQueryServiceStateImpl service = new UserActionCountQueryServiceStateImpl();
        // 构造几个明细事件
        LogBean logBean = new LogBean();
        logBean.setEventId("000010");
        HashMap<String, String> properties = new HashMap<>();
        properties.put("p1", "v1");
        logBean.setProperties(properties);

        LogBean logBean2 = new LogBean();
        logBean2.setEventId("000010");
        HashMap<String, String> properties2 = new HashMap<>();
        properties2.put("p1", "v2");
        logBean2.setProperties(properties2);

        LogBean logBean3 = new LogBean();
        logBean3.setEventId("000020");
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("p2", "v3");
        logBean3.setProperties(properties3);

        LogBean logBean4 = new LogBean();
        logBean4.setEventId("000020");
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("p2", "v3");
        properties4.put("p3", "v4");
        logBean4.setProperties(properties4);


        ArrayList<LogBean> eventList = new ArrayList<>();
        eventList.add(logBean);
        eventList.add(logBean2);
        eventList.add(logBean3);
        eventList.add(logBean4);

        // 构造2个规则原子条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("000010");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1", "v1");
        param1.setProperties(paramProps1);
        param1.setThreshold(2);

        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("000020");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2", "v3");
        param2.setProperties(paramProps2);
        param2.setThreshold(2);

        ArrayList<RuleAtomicParam> ruleAtomicParams = new ArrayList<>();
        ruleAtomicParams.add(param1);
        ruleAtomicParams.add(param2);

        service.queryActionCountsHelper(eventList, ruleAtomicParams);
        for (RuleAtomicParam ruleAtomicParam : ruleAtomicParams) {
            System.out.println(ruleAtomicParam.getEventId() + "," + ruleAtomicParam.getThreshold() + "," + ruleAtomicParam.getRealCounts());
        }
    }
}
