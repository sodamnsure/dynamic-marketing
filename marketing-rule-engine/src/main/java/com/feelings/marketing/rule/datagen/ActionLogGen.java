package com.feelings.marketing.rule.datagen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.feelings.marketing.rule.pojo.LogBean;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

/**
 * @Author: sodamnsure
 * @Date: 2021/5/25 8:13 下午
 * @desc: 行为日志生成模拟器
 */
public class ActionLogGen {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();

        while (true) {
            LogBean logBean = new LogBean();
            // logBean设置account: 生成1到10000之间的随机数，不足6位补齐0
            String account = StringUtils.leftPad(RandomUtils.nextInt(1, 10000) + "", 6, "0");
            logBean.setAccount(account);
            // logBean设置appId
            logBean.setAppId("com.feelings.marketing");
            // logBean设置版本
            logBean.setAppVersion("2.5");
            // logBean设置运营商
            logBean.setCarrier("中国移动");
            // logBean设置设备ID
            logBean.setDeviceId(account);
            // logBean设置设备类型
            logBean.setDeviceType("mi6");
            // logBean设置IP
            logBean.setIp("172.247.129.103");
            // logBean设置经度
            logBean.setLatitude(RandomUtils.nextDouble(120.0, 160.0));
            // logBean设置纬度:
            logBean.setLongitude(RandomUtils.nextDouble(10.0, 52.0));
            // logBean设置网络类型
            logBean.setNetType("5G");
            // logBean设置操作系统
            logBean.setOsName("android");
            // logBean设置操作系统版本
            logBean.setOsVersion("7.5");
            // logBean设置发布渠道
            logBean.setReleaseChannel("应用宝");
            // logBean设置分辨率
            logBean.setResolution("2048*1024");
            // logBean设置sessionId: 随机生成最小长度最大长度为10的字符串
            logBean.setSessionId(RandomStringUtils.randomNumeric(10, 10));
            // logBean设置timeStamp
            logBean.setTimeStamp(System.currentTimeMillis());
            // logBean设置eventId: 随机生成26个英文字母当中的一个
            logBean.setEventId(RandomStringUtils.randomAlphabetic(1));
            // logBean设置properties
            HashMap<String, String> properties = new HashMap<>();
            for (int i = 0; i < RandomUtils.nextInt(1, 5); i++) {
                properties.put("p" + RandomUtils.nextInt(1, 10), "v" + RandomUtils.nextInt(1, 10));
            }
            logBean.setProperties(properties);

            String log = mapper.writeValueAsString(logBean);
            System.out.println(log);
            Thread.sleep(RandomUtils.nextInt(500, 3000));
        }

    }
}
