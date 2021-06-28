package com.feelings.marketing.rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/2 3:35 下午
 * @desc: 规则参数中的原子条件封装实体
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleAtomicParam implements Serializable {
    // 事件类型
    private String eventId;
    // 事件属性要求
    private HashMap<String, String> properties;
    // 阈值要求
    private int threshold;
    // 要求的事件发生时间段起始时间
    private Long rangeStart;
    // 要求的事件发生时间段结束时间
    private Long rangeEnd;
    // 条件对应的ClickHouse查询SQL
    private String countQuerySql;
    // 用于记录查询服务所返回的查询值
    private int realCounts;
}
