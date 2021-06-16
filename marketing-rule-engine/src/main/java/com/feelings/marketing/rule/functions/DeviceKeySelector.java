package com.feelings.marketing.rule.functions;


import com.feelings.marketing.rule.pojo.LogBean;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/16 5:47 下午
 */
public class DeviceKeySelector implements KeySelector<LogBean, String> {

    @Override
    public String getKey(LogBean logBean) throws Exception {
        return logBean.getDeviceId();
    }
}
