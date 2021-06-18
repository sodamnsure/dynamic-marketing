package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.utils.RuleCalcUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 2:55 下午
 * @desc: 用户行为次序类条件查询服务实现（在 state 中查询）
 */
public class UserActionSeqQueryServiceStateImpl implements UserActionSeqQueryService {
    /**
     * 查询规则条件中的 行为序列条件
     * 会将查询到的最大匹配步骤， set回 ruleParam对象中
     *
     * @param eventState flink中存储用户事件明细的state
     * @param ruleParam  规则参数对象
     * @return 返回成立与否
     */
    @Override
    public boolean queryActionSeq(ListState<LogBean> eventState, RuleParam ruleParam) throws Exception {
        Iterable<LogBean> logBeans = eventState.get();
        List<RuleAtomicParam> userActionSeqParams = ruleParam.getUserActionSeqParams();
        // 调用helper方法统计实际匹配的最大步骤号
        int maxStep = queryActionSeqHelper(logBeans, userActionSeqParams);
        // 将maxStep丢回规则参数对象，以便调用者可以根据需要获取到这个最大匹配步骤号
        ruleParam.setUserActionSeqQueriedMaxStep(maxStep);
        // 然后判断整个序列条件是否满足：真实最大匹配步骤是否等于条件的步骤数
        return maxStep == userActionSeqParams.size();
    }

    /**
     * 统计明细事件中，与序列条件匹配到的最大步骤
     *
     * @param events
     * @param userActionSeqParams
     * @return
     */
    public int queryActionSeqHelper(Iterable<LogBean> events, List<RuleAtomicParam> userActionSeqParams) {
        ArrayList<LogBean> eventList = new ArrayList<>();
        CollectionUtils.addAll(eventList, events.iterator());
        // 外循环，遍历每一个次序条件
        int maxStep = 0;
        int index = 0;
        for (RuleAtomicParam userActionSeqParam : userActionSeqParams) {
            // 内循环，遍历每一个明细事件
            boolean isFind = false;
            for (int i = index; i < eventList.size(); i++) {
                LogBean logBean = eventList.get(i);
                // 判断当前事件是否满足当前次序条件
                boolean match = RuleCalcUtil.eventBeanMatchEventParam(logBean, userActionSeqParam, true);
                // 如果匹配，则最大步骤号+1，且更新下一次内循环的起始位置
                if (match) {
                    maxStep++;
                    index = i + 1;
                    isFind = true;
                    break;
                }
            }
            // 当某一个次序条件没有的时候，直接跳出外循环
            if (!isFind) {
                break;
            }

        }
        return maxStep;
    }

    /**
     * 序列匹配，性能改进版
     * @param events
     * @param userActionSeqParams
     * @return
     */
    public int queryActionSeqHelper2(Iterable<LogBean> events, List<RuleAtomicParam> userActionSeqParams) {
        int maxStep = 0;
        for (LogBean event : events) {
            // 遍历event，查看和用户的第maxStep是否一致
            if (RuleCalcUtil.eventBeanMatchEventParam(event, userActionSeqParams.get(maxStep))) {
                maxStep++;
            }
            if (maxStep == userActionSeqParams.size()) break;
        }
        return maxStep;
    }
}
