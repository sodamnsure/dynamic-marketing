package com.feelings.marketing.rule.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/16 4:56 下午
 */
public class JsonUtils {
    // 定义JackSon对象
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 将对象转换成Json字符串
     * @param data
     * @return
     */
    public static String objectToJson(Object data) {
        try {
            String string = MAPPER.writeValueAsString(data);
            return string;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将Json数据转换成Pojo对象List
     * @param jsonData
     * @param beanType
     * @param <T>
     * @return
     */
    public static <T> T jsonToPojo(String jsonData, Class<T> beanType) {
        try {
            T t = MAPPER.readValue(jsonData, beanType);
            return t;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将Json数据转化成Pojo对象List
     * @param jsonData
     * @param beanType
     * @param <T>
     * @return
     */
    public static <T> List<T> jsonToList(String jsonData, Class<T> beanType) {
        JavaType javaType = MAPPER.getTypeFactory().constructParametricType(List.class, beanType);
        try {
            List<T> list = MAPPER.readValue(jsonData, javaType);
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
