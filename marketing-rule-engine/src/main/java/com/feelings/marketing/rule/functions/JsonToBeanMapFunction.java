package com.feelings.marketing.rule.functions;

import com.feelings.marketing.rule.pojo.LogBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/9 6:05 下午
 */
public class JsonToBeanMapFunction implements MapFunction<String, LogBean> {

    @Override
    public LogBean map(String s) throws Exception {
        return null;
    }
}
