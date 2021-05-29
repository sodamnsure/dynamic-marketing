package com.feelings.marketing.rule.engine;

/**
 * @Author: sodamnsure
 * @Date: 2021/5/28 2:27 下午
 * @desc: 实时运营系统版本1.0
 *
 * 规则：
 *  触发条件：E事件
 *  画像属性条件：k3=v3, k100=v80, k230=v360
 *  行为属性条件：U(p1=v3, p2=v2) >= 3次 且 G(p6=v8, p4=v5, p1=v2) >= 1次
 *  行为次序条件：依次做过--> W(p1=v4) --> R(p2 = v3) --> F
 */
public class RuleEngineV1 {
    public static void main(String[] args) {

    }
}
