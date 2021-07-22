package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.BufferAvailableLevel;
import com.feelings.marketing.rule.pojo.BufferResult;
import redis.clients.jedis.Jedis;

/**
 * @Author: sodamnsure
 * @Date: 2021/7/22 3:16 下午
 * @desc: 缓存管理器
 */
public class BufferManager {
    Jedis jedis;

    public BufferManager() {
        jedis = new Jedis("feelings", 6379);
    }

    // 获取缓存中的数据
    public BufferResult getBufferData(String bufferKey, Long paramRangeStart, Long paramRangeEnd, int threshold) {
        BufferResult bufferResult = new BufferResult();

        bufferResult.setBufferAvailableLevel(BufferAvailableLevel.UN_AVL);

        // data: 2|t1,t8
        String data = jedis.get(bufferKey);
        String[] split = data.split("\\|");
        String[] timeRange = split[1].split(",");


        bufferResult.setBufferKey(bufferKey);
        bufferResult.setBufferValue(Integer.parseInt(split[0]));
        bufferResult.setBufferRangeStart(Long.parseLong(timeRange[0]));
        bufferResult.setBufferRangeStart(Long.parseLong(timeRange[1]));

        if (paramRangeStart <= bufferResult.getBufferRangeStart()
                && paramRangeEnd >= bufferResult.getBufferRangeEnd()
                && bufferResult.getBufferValue() >= threshold) {
            bufferResult.setBufferAvailableLevel(BufferAvailableLevel.WHOLE_AVL);
        }

        // 起始点对齐，但是结束点比规则中的要小
        if (paramRangeStart.equals(bufferResult.getBufferRangeStart()) && paramRangeEnd >= bufferResult.getBufferRangeEnd()) {
            bufferResult.setBufferAvailableLevel(BufferAvailableLevel.PARTIAL_AVL);
            // 更新外部后续查询的条件窗口起始点
            bufferResult.setOutSideQueryStart(bufferResult.getBufferRangeEnd());
        }

        return bufferResult;
    }

    // 插入数据到缓存
    public void putBufferData(String bufferKey, Integer bufferValue, Long bufferRangeStart, Long bufferRangeEnd) {
        jedis.setex(bufferKey, 4*60*60*1000,bufferValue + "|" + bufferRangeStart + "," + bufferRangeEnd);
    }

    // 更新已存在的缓存数据


    // 删除已存在的缓存数据
}
