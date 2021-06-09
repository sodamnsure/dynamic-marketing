package com.feelings.marketing.rule.moduletest;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.service.UserActionSeqQueryServiceStateImpl;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 11:19 上午
 */
public class ActionSeqQueryTest {
    public static void main(String[] args) {
        // 构造几个明细事件
        LogBean logBean = new LogBean();
        logBean.setEventId("000010");
        HashMap<String, String> properties = new HashMap<>();
        properties.put("p1", "v1");
        logBean.setProperties(properties);

        LogBean logBean5 = new LogBean();
        logBean5.setEventId("000020");
        HashMap<String, String> properties5 = new HashMap<>();
        properties5.put("p2", "v3");
        properties5.put("p3", "v4");
        logBean5.setProperties(properties5);

        LogBean logBean2 = new LogBean();
        logBean2.setEventId("000310");
        HashMap<String, String> properties2 = new HashMap<>();
        properties2.put("p1", "v2");
        logBean2.setProperties(properties2);

        LogBean logBean3 = new LogBean();
        logBean3.setEventId("000020");
        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("p2", "v3");
        properties3.put("p4", "v5");
        logBean3.setProperties(properties3);

        LogBean logBean4 = new LogBean();
        logBean4.setEventId("000022");
        HashMap<String, String> properties4 = new HashMap<>();
        properties4.put("p2", "v3");
        properties4.put("p3", "v4");
        logBean4.setProperties(properties4);



        ArrayList<LogBean> eventList = new ArrayList<>();
        eventList.add(logBean);
        eventList.add(logBean5);
        eventList.add(logBean2);
        eventList.add(logBean3);
        eventList.add(logBean4);

        // 构造一个序列条件
        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("000010");
        HashMap<String, String> paramProps1 = new HashMap<>();
        paramProps1.put("p1", "v1");
        param1.setProperties(paramProps1);


        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("000020");
        HashMap<String, String> paramProps2 = new HashMap<>();
        paramProps2.put("p2", "v3");
        param2.setProperties(paramProps2);


        RuleAtomicParam param3 = new RuleAtomicParam();
        param3.setEventId("000020");
        HashMap<String, String> paramProps3 = new HashMap<>();
        paramProps3.put("p4", "v5");
        param3.setProperties(paramProps3);


        ArrayList<RuleAtomicParam> ruleAtomicParams = new ArrayList<>();
        ruleAtomicParams.add(param1);
        ruleAtomicParams.add(param2);
        ruleAtomicParams.add(param3);

        // 调用Service服务计算
        UserActionSeqQueryServiceStateImpl service = new UserActionSeqQueryServiceStateImpl();
        int maxStep = service.queryActionSeqHelper(eventList, ruleAtomicParams);
        System.out.println(maxStep);

    }


}
