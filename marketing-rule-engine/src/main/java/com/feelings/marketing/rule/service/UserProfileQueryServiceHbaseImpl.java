package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.RuleParam;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:07 上午
 * @desc: 用户画像查询服务：Hbase查询实现类
 */
public class UserProfileQueryServiceHbaseImpl implements UserProfileQueryService {
    /**
     * 传入一个用户好，以及要查询的条件
     * 返回这些条件是否满足
     * @param deviceId
     * @param ruleParam
     * @return
     */
    @Override
    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam) {
        return false;
    }
}
