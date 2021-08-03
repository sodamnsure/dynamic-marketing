package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.BufferResult;
import com.feelings.marketing.rule.pojo.LogBean;
import com.feelings.marketing.rule.pojo.RuleAtomicParam;
import com.feelings.marketing.rule.pojo.RuleParam;
import com.feelings.marketing.rule.utils.RuleCalcUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/9 4:33 下午
 * @desc: 查询路由模块（加入缓存后的版本）
 * 核心计算入口类:
 * 1. 先用缓存管理器去查询缓存数据
 * 2. 然后再根据情况调用各类服务去计算
 * 3. 还要将计算结果更新到缓存中
 */
public class QueryRouterV4 {
    // 画像查询服务
    private UserProfileQueryService userProfileQueryService;

    // 次数类条件查询服务
    private UserActionCountQueryService userActionCountQueryStateService;
    private UserActionSeqQueryService userActionSeqQueryStateService;

    // 次序类条件查询服务
    private UserActionCountQueryService userActionCountQueryClickHouseService;
    private UserActionSeqQueryService userActionSeqQueryClickHouseService;

    // 缓存管理器
    BufferManager bufferManager;

    /**
     * 构造方法
     * 创建各类服务实例和缓存管理实例
     *
     * @throws Exception
     */
    public QueryRouterV4() throws Exception {
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

        /**
         * 构造一个缓存管理器
         */
        bufferManager = new BufferManager();

    }

    /**
     * 控制画像条件查询路由
     *
     * @param logBean
     * @param ruleParam
     * @return
     */
    public boolean profileQuery(LogBean logBean, RuleParam ruleParam) {
        System.out.println("开始查询画像条件");

        // 查询画像条件
        boolean profileIfMatch = userProfileQueryService.judgeProfileCondition(logBean.getEventId(), ruleParam);
        if (!profileIfMatch) return false;
        return true;
    }

    /**
     * 控制次数条件查询路由
     *
     * @param logBean
     * @param ruleParam
     * @param eventState
     * @return
     * @throws Exception
     */
    public boolean countConditionQuery(LogBean logBean, RuleParam ruleParam, ListState<LogBean> eventState) throws Exception {
        // 取出规则中的次数类条件
        List<RuleAtomicParam> userActionCountParams = ruleParam.getUserActionCountParams();

        /**
         * 处理缓存查询
         *
         * 遍历规则，逐个条件去缓存中查询
         * 依据查询后结果，如果已经完全匹配的，从规则中直接剔除
         * 如果部分有效，则将条件的时间窗口起始点更新为缓存有效窗口的结束点
         */
        updateRuleParamByBufferResult(logBean, userActionCountParams);


        // 计算查询分界点timestamp
        // 当前时间对小时取整减1
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();


        // 遍历规则中的次数类条件，按照时间跨度，分成三组
        ArrayList<RuleAtomicParam> forwardRangeParams = new ArrayList<>();  // 只查远期的条件组
        ArrayList<RuleAtomicParam> nearRangeParams = new ArrayList<>(); // 只查近期的条件组
        ArrayList<RuleAtomicParam> crossRangeParams = new ArrayList<>(); // 跨界条件组

        // 调用方法划分
        allocateParamList(userActionCountParams, splitPoint, forwardRangeParams, nearRangeParams, crossRangeParams);

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

        /**
         * 查询远期条件组
         */
        if (forwardRangeParams.size() > 0) {
            // 将规则总参数中对象中的"次数类条件"覆盖成：远期条件组
            ruleParam.setUserActionCountParams(forwardRangeParams);
            boolean b = userActionCountQueryClickHouseService.queryActionCounts(logBean.getDeviceId(), null, ruleParam);
            if (!b) return false;
        }

        return true;
    }

    /**
     * 控制次序条件查询路由
     *
     * @param logBean
     * @param ruleParam
     * @param eventState
     * @return
     * @desc: 先查near，得到结果maxStep，如果满足则结束
     * 如果不满足，则查far得到结果x,如果已满足则结束，如果x不满足，则再在near查条件中去掉x个之步骤后的序列得到结果y,最终返回x+y
     */
    public boolean seqConditionQuery(LogBean logBean, RuleParam ruleParam, ListState<LogBean> eventState) throws Exception {
        // 取出规则中的序列条件
        List<RuleAtomicParam> userActionSeqParams = ruleParam.getUserActionSeqParams();
        // 如果序列条件为空，则直接返回true
        if (userActionSeqParams == null || userActionSeqParams.size() < 1) return true;

        // 取出序列条件相关参数，取得条件的初始时间、结束时间和总步骤数
        Long paramStart = userActionSeqParams.get(0).getRangeStart();
        Long paramEnd = userActionSeqParams.get(0).getRangeEnd();
        // 取出规则中的序列的总步骤数
        Integer totalSteps = userActionSeqParams.size();

        /**
         * 处理缓存查询
         *
         *  依据查询后的结果，如果已经完成匹配，从规则中直接剔除
         *  如果是部分有效，则将条件中的时间窗口起始点更新为缓存有效窗口的结束点
         */
        // 拼接bufferKey
        String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), userActionSeqParams);

        BufferResult bufferResult = bufferManager.getBufferData(bufferKey, paramStart, paramEnd, totalSteps);

        // 判断有效性
        switch (bufferResult.getBufferAvailableLevel()) {
            case PARTIAL_AVL: // 缓存部分有效，则设置maxStep到参数中,并截短条件序列及条件时间窗口
                // 如果是部分有效，则更新条件的窗口起始点
                userActionSeqParams.get(0).setRangeStart(bufferResult.getBufferRangeEnd());
                // 将缓存value值，放入参数对象的maxStep中
                ruleParam.setUserActionSeqQueriedMaxStep(bufferResult.getBufferValue());
                // 截短条件序列
                List<RuleAtomicParam> newSeqList = userActionSeqParams.subList(bufferResult.getBufferValue(), totalSteps);
                ruleParam.setUserActionSeqParams(newSeqList);
                break;
            case UN_AVL:
                break;
            case WHOLE_AVL:
                return true;

        }


        // 计算查询分界点timestamp
        // 当前时间对小时取整减1
        long splitPoint = DateUtils.addHours(DateUtils.ceiling(new Date(), Calendar.HOUR), -2).getTime();

        userActionSeqParams = ruleParam.getUserActionSeqParams();

        // 如果序列有内容，才开始计算
        if (userActionSeqParams != null && userActionSeqParams.size() > 0) {
            Long rangeStart = userActionSeqParams.get(0).getRangeStart();
            Long rangeEnd = userActionSeqParams.get(0).getRangeEnd();
            // 如果条件的时间窗口起始时间大于等于分界点，则在state查询
            if (rangeStart >= splitPoint) {
                boolean b = userActionSeqQueryStateService.queryActionSeq("", eventState, ruleParam);
                return b;
            } else if (rangeStart < splitPoint && rangeEnd > splitPoint) { // 否则跨界查询
                // 重设时间窗口，先查state
                modifyTimeRange(userActionSeqParams, splitPoint, rangeEnd);
                // 
                int bufferValue = ruleParam.getUserActionSeqQueriedMaxStep();
                userActionSeqQueryStateService.queryActionSeq(logBean.getDeviceId(), eventState, ruleParam);
                if (ruleParam.getUserActionSeqQueriedMaxStep() >= totalSteps) {
                    return true;
                } else {
                    // 将参数中的maxStep恢复到缓存查完后的值
                    ruleParam.setUserActionSeqQueriedMaxStep(bufferValue);
                }

                // 如果state中没有查询到，则按照正统思路查（先查clickhouse， 再查state，再整合结果）
                // 更新时间段
                modifyTimeRange(userActionSeqParams, rangeStart, splitPoint);

                // 交给clickhouse service去查远期部分
                userActionSeqQueryClickHouseService.queryActionSeq(logBean.getDeviceId(), eventState, ruleParam);
                int farMaxStep = ruleParam.getUserActionSeqQueriedMaxStep();
                if (ruleParam.getUserActionSeqQueriedMaxStep() >= totalSteps) {
                    return true;
                }

                // 如果远期部分不足以满足整个条件，则将条件截短
                modifyTimeRange(userActionSeqParams, splitPoint, rangeEnd);
                // 截短序列
                ruleParam.setUserActionSeqParams(userActionSeqParams.subList(farMaxStep, userActionSeqParams.size()));
                // 查询
                userActionSeqQueryStateService.queryActionSeq(logBean.getDeviceId(), eventState, ruleParam);
                int nearMaxStep = ruleParam.getUserActionSeqQueriedMaxStep();

                // 整合最终结果，塞回参数对象
                ruleParam.setUserActionSeqQueriedMaxStep(farMaxStep + nearMaxStep);
                return farMaxStep + nearMaxStep >= totalSteps;
            } else {  // 如果条件的时间窗口结束时间小于分界点，则在clickhouse查询
                boolean b = userActionSeqQueryClickHouseService.queryActionSeq(logBean.getDeviceId(), null, ruleParam);
                return b;
            }
        }
        return true;
    }

    /**
     * 更新条件时间窗口起始点的工具方法
     *
     * @param userActionSeqParams
     * @param newStart
     * @param newEnd
     */
    private void modifyTimeRange(List<RuleAtomicParam> userActionSeqParams, long newStart, long newEnd) {
        for (RuleAtomicParam userActionSeqParam : userActionSeqParams) {
            userActionSeqParam.setRangeStart(newStart);
            userActionSeqParam.setRangeEnd(newEnd);
        }
    }

    /**
     * 根据缓存，并根据缓存结果情况，更新原始规则条件
     * 1. 缩短条件的窗口
     * 2. 剔除条件
     * 3. 什么都不错
     *
     * @param logBean               待处理条件
     * @param userActionCountParams 次数类条件list
     */
    private void updateRuleParamByBufferResult(LogBean logBean, List<RuleAtomicParam> userActionCountParams) {
        for (int i = 0; i < userActionCountParams.size(); i++) {
            // 从条件列表中取出条件i
            RuleAtomicParam countParam = userActionCountParams.get(i);
            // 拼接bufferKey
            String bufferKey = RuleCalcUtil.getBufferKey(logBean.getDeviceId(), countParam);
            // 从redis中取数据
            BufferResult bufferResult = bufferManager.getBufferData(bufferKey, countParam);
            // 判断有效性
            switch (bufferResult.getBufferAvailableLevel()) {
                // 如果部分有效
                case PARTIAL_AVL:
                    // 则更新规则条件的窗口起始点
                    countParam.setRangeStart(bufferResult.getBufferRangeEnd());
                    // 将缓存value值，放入参数对象的真实值中
                    countParam.setRealCounts(bufferResult.getBufferValue());
                    break;
                // 如果完全有效，则剔除条件
                case WHOLE_AVL:
                    userActionCountParams.remove(i);
                    i--;
                    break;
                case UN_AVL:

            }
        }
    }


    /**
     * @param userActionCountParams 初始条件list
     * @param splitPoint            时间分界点
     * @param forwardRangeParams    远期条件组
     * @param nearRangeParams       近期条件组
     * @param crossRangeParams      跨界条件组
     */
    private void allocateParamList(List<RuleAtomicParam> userActionCountParams, long splitPoint, ArrayList<RuleAtomicParam> forwardRangeParams, ArrayList<RuleAtomicParam> nearRangeParams, ArrayList<RuleAtomicParam> crossRangeParams) {
        for (RuleAtomicParam userActionCountParam : userActionCountParams) {
            if (userActionCountParam.getRangeEnd() < splitPoint) {
                // 如果条件结束时间小于分界点，放入远期条件组
                forwardRangeParams.add(userActionCountParam);
            } else if (userActionCountParam.getRangeStart() >= splitPoint) {
                // 如果条件起始时间大于等于分界点，放入近期条件组
                nearRangeParams.add(userActionCountParam);
            } else {
                // 否则，放入跨界条件组
                crossRangeParams.add(userActionCountParam);
            }
        }
    }
}
