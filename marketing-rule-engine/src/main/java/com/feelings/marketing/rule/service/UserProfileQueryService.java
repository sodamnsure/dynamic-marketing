package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.RuleParam;

import java.io.IOException;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:04 上午
 * @desc: 用户画像数据查询服务接口
 */
public interface UserProfileQueryService {

    boolean judgeProfileCondition(String deviceId, RuleParam ruleParam);
}
