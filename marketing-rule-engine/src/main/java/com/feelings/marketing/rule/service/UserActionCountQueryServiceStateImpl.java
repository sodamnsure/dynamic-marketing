package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.flink.api.common.state.ListState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:17 上午
 * @desc: 用户行为次数类条件查询服务实现：在flink的state中统计行为次数
 */
public class UserActionCountQueryServiceStateImpl implements UserActionCountQueryService {
    /**
     * 查询规则参数对象中，要求的用户行为次数类条件是否满足
     * 同时将查询到的真实次数 set 回规则参数对象中
     * @param eventState 用户事件明确存储state
     * @param ruleParam 规则整体参数对象
     * @return 条件是否满足
     */
    @Override
    public boolean queryActionCounts(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {

        // 取出用户行为次数类条件
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();
        // 迭代每一个历史明细事件
        Iterable<LogBean> logBeansIterable = eventState.get();
        // 统计每一个原子条件所发生的真实次数
        queryActionCountsHelper(logBeansIterable, userActionCountParams);
        // 经过上面的方法执行后，每一个原子条件中，都拥有了一个真实发生次数，在下面判断是否每个原子条件都满足
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            if (userActionCountParam.getRealCounts() < userActionCountParam.getThreshold()) {
                return false;
            }
        }
        // 如果到达这一句话，说明上面的判断中，每个原子条件都满足，返回整体结果true
        return true;
    }

    /**
     * 根据传入的历史明细和规则条件
     * 挨个统计每个规则原子条件的真实发生次数，并将结果set回规则条件参数中
     * @param logBeansIterable
     * @param userActionCountParams
     */
    public void queryActionCountsHelper(Iterable<LogBean> logBeansIterable, List<RuleAtomicParam> userActionCountParams) {
        for (LogBean logBean : logBeansIterable) {
            for (RuleAtomicParam userActionCountParam : userActionCountParams) {
                // 判断当前logBean和当前原子条件userActionCountParam是否一致
                boolean isMatch = eventBeanMatchEventParam(logBean, userActionCountParam);
                // 如果一致，则查询次数结果加一
                if (isMatch) {
                    userActionCountParam.setRealCounts(userActionCountParam.getRealCounts() + 1);
                }
            }
        }
    }

    /**
     * 工具方法：用于判断一个带判断事件和一个规则中的原子条件是否一致
     * @param eventBean
     * @param eventParam
     * @return
     */
    private boolean eventBeanMatchEventParam(LogBean eventBean, RuleAtomicParam eventParam) {

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
}
