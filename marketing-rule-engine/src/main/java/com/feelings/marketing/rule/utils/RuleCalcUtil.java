package com.feelings.marketing.rule.utils;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 10:40 上午
 * @desc: 规则计算中用到的各类工具方法
 */
public class RuleCalcUtil {
    /**
     * 工具方法：用于判断一个带判断事件和一个规则中的原子条件是否一致
     *
     * @param eventBean
     * @param eventParam
     * @return
     */
    public static boolean eventBeanMatchEventParam(LogBean eventBean, RuleAtomicParam eventParam) {

        // 如果传入的事件ID与参数中的事件ID相同, 如果不相同，直接返回false
        if (eventBean.getEventId().equals(eventParam.getEventId())) {
            // 取出待判断事件中的属性
            Map<String, String> eventBeanProperties = eventBean.getProperties();
            // 取出条件中的事件属性
            HashMap<String, String> eventParamProperties = eventParam.getProperties();
            // 遍历条件中每个属性和值
            Set<Map.Entry<String, String>> entries = eventParamProperties.entrySet();
            for (Map.Entry<String, String> entry : entries) {
                // 判断条件中的属性值是否等于事件中带判断条件的属性值，如果不相等，直接返回false
                if (!entry.getValue().equals(eventBeanProperties.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    public static boolean eventBeanMatchEventParam(LogBean eventBean, RuleAtomicParam eventParam, boolean needTimeCompare) {
        boolean b = eventBeanMatchEventParam(eventBean, eventParam);
        // 要考虑一点，外部传入的条件中，时间范围条件，如果起始、结束没有约束，应该传入一个 -1
        Long rangeStart = eventParam.getRangeStart();
        Long rangeEnd = eventParam.getRangeEnd();
        return b && eventBean.getTimeStamp() >= (rangeStart == -1 ? 0 : rangeStart) && eventBean.getTimeStamp() <= (rangeEnd == -1 ? Long.MAX_VALUE : rangeEnd);
    }


    public static String getBufferKey(String deviceId, RuleAtomicParam ruleAtomicParam) {
        // deviceId-EVENT-p1-v1-p2-v2
        StringBuffer sb = new StringBuffer();
        sb.append(deviceId).append("-").append(ruleAtomicParam.getEventId());

        HashMap<String, String> properties = ruleAtomicParam.getProperties();
        Set<Map.Entry<String, String>> entries = properties.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            sb.append("-").append(entry.getKey()).append("-").append(entry.getValue());
        }
        return sb.toString();
    }

    public static String getBufferKey(String deviceId, List<RuleAtomicParam> ruleAtomicParams) {
        // deviceId-EVENT-p1-v1-p2-v2
        StringBuffer sb = new StringBuffer();
        sb.append(deviceId).append("-");

        for (RuleAtomicParam ruleAtomicParam : ruleAtomicParams) {
            sb.append("-").append(ruleAtomicParam.getEventId());
            HashMap<String, String> properties = ruleAtomicParam.getProperties();
            Set<Map.Entry<String, String>> entries = properties.entrySet();
            for (Map.Entry<String, String> entry : entries) {
                sb.append("-").append(entry.getKey()).append("-").append(entry.getValue());
            }
        }

        return sb.toString();
    }


}
