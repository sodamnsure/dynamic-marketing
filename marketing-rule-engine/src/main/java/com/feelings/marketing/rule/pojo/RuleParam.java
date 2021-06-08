package com.feelings.marketing.rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 10:52 上午
 * @desc: 规则整体条件封装实体
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleParam implements Serializable {
    // 规则中的触发条件
    private RuleAtomicParam triggerParam;
    // 规则中的用户画像条件
    private HashMap<String, String> userProfileParams;
    // 规则中的行为属性条件
    private List<RuleAtomicParam> userActionCountParams;
    // 规则中的行为次序条件
    private List<RuleAtomicParam> userActionSeqParams;
    // 用于记录查询服务所返回的序列中匹配的最大步骤好
    private int userActionSeqQueriedMaxStep;
}
