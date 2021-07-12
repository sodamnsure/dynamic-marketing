package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/9 4:33 下午
 * @desc: 查询路由模块
 */
public class QueryRouter {
    private UserProfileQueryService userProfileQueryService;

    private UserActionCountQueryService userActionCountQueryStateService;
    private UserActionSeqQueryService userActionSeqQueryStateService;

    private UserActionCountQueryService userActionCountQueryClickHouseService;
    private UserActionSeqQueryService userActionSeqQueryClickHouseService;

    public QueryRouter() throws Exception {
        userProfileQueryService = new UserProfileQueryServiceHbaseImpl();

        /**
         * 构造底层的核心State查询服务
         */
        userActionCountQueryStateService = new UserActionCountQueryServiceStateImpl();
        userActionSeqQueryStateService = new UserActionSeqQueryServiceStateImpl();

        /**
         * 构造底层的核心ClickHouse查询服务
         */
        userActionCountQueryClickHouseService = new UserActionCountQueryServiceClickHouseImpl();
        userActionSeqQueryClickHouseService = new UserActionSeqQueryServiceClickHouseImpl();
    }

    // 控制画像条件查询路由
    public boolean profileQuery(LogBean logBean, RuleParam ruleParam) {
        // 查询画像条件
        boolean profileIfMatch = userProfileQueryService.judgeProfileCondition(logBean.getEventId(), ruleParam);
        if (!profileIfMatch) return false;
        return true;
    }

    // 控制次数条件查询路由
    public boolean countConditionQuery(LogBean logBean, RuleParam ruleParam, ListState<LogBean> eventState) throws Exception {
        // 计算查询分界点timestamp
        // 当前时间对小时取整减1
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

        // 遍历规则中的次数类条件，按照时间跨度，分成三组
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        ArrayList<RuleAtomicParam> forwardRangeParams = new ArrayList<>();  // 只查远期的条件组
        ArrayList<RuleAtomicParam> nearRangeParams = new ArrayList<>(); // 只查近期的条件组
        ArrayList<RuleAtomicParam> crossRangeParams = new ArrayList<>(); // 跨界条件组

        // 条件分组
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            if (userActionCountParam.getRangeEnd() < splitPoint) {
                // 如果条件结束时间小于分界点，放入远期条件组
                forwardRangeParams.add(userActionCountParam);
            } else if (userActionCountParam.getRangeStart() >= splitPoint){
                // 如果条件起始时间大于等于分界点，放入近期条件组
                nearRangeParams.add(userActionCountParam);
            } else {
                // 否则，放入跨界条件组
                crossRangeParams.add(userActionCountParam);
            }
        }

        /**
         * 查询近期条件组
         */
        if (nearRangeParams.size() > 0) {
            // 将规则总参数对象中的"次数条件"覆盖成：近期条件组
            ruleParam.setUserActionCountParams(nearRangeParams);
            // 交给stateService，对这一组条件进行计算
            boolean countMatch = userActionCountQueryStateService.queryActionCounts("", eventState, ruleParam);
            if (!countMatch) return false;
        }

        /**
         * 查询远期条件组
         */
        if (forwardRangeParams.size() > 0) {
            // 将规则总参数中对象中的"次数类条件"覆盖成：远期条件组
            ruleParam.setUserActionCountParams(forwardRangeParams);
            boolean b = userActionCountQueryClickHouseService.queryActionCounts(logBean.getDeviceId(), null, ruleParam);
            if (!b) return false;
        }

        /**
         * 查询跨界条件组
         */
        RuleParam copyParamRight = new RuleParam(); // 分界点右边分段的count参数
        RuleParam copyParamLeft = new RuleParam();  // 分界点左边分段的count参数
        for (RuleAtomicParam crossRangeParam : crossRangeParams) {
            long originRangeStart = crossRangeParam.getRangeStart();

            // 将对象的rangeStart换成分界点，去stateService中查询
            crossRangeParam.setRangeStart(splitPoint);
            boolean b = userActionCountQueryStateService.queryActionCounts(logBean.getDeviceId(), eventState, crossRangeParam);
            if (b) continue;  // 如果近期条件满足，则不再判断远期情况
            // 如果上面不满足，则将rangeEnd换成分界点，去clickhouse service查询
            crossRangeParam.setRangeStart(originRangeStart);
            crossRangeParam.setRangeEnd(splitPoint);
            boolean b1 = userActionCountQueryClickHouseService.queryActionCounts(logBean.getDeviceId(), eventState, crossRangeParam);

            if (!b1) return false;
        }
        return true;
    }

    // 控制次序条件查询路由
}
