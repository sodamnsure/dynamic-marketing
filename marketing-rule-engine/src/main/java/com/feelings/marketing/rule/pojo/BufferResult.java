package com.feelings.marketing.rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/22 3:19 下午
 * @desc: 封装从缓存中查询到的结果的实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BufferResult {
    // 缓存结果所对应的Key
    private String bufferKey;
    // 缓存结果所对应的Value
    private Integer bufferValue;
    // 缓存数据的窗口起始时间
    private Long bufferRangeStart;
    // 缓存数据的窗口结束时间
    private Long bufferRangeEnd;
    // 缓存结果的有效性等级
    private BufferAvailableLevel bufferAvailableLevel;
    // 调整后的后续查询窗口的起始点
    private Long outSideQueryStart;
}
