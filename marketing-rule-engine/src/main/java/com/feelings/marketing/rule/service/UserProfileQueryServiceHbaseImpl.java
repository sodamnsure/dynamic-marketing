package com.feelings.marketing.rule.service;

import com.feelings.marketing.rule.pojo.RuleParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/5 11:07 上午
 * @desc: 用户画像查询服务：Hbase查询实现类
 */
public class UserProfileQueryServiceHbaseImpl implements UserProfileQueryService {
    Connection conn;
    Table table;

    /**
     * 在构造函数中创建链接
     */
    public UserProfileQueryServiceHbaseImpl() throws IOException {
        // 创建HBASE配置
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "feelings:2181");
        // 创建HBASE链接
        conn = ConnectionFactory.createConnection(conf);
        // 创建链接后要拿到表
        table = conn.getTable(TableName.valueOf("user_profile"));
    }


    /**
     * 传入一个用户好，以及要查询的条件
     * 返回这些条件是否满足
     * TODO 本查询只返回了是与否，而查询到的画像数据并没有返回
     * TODO 可能为将来的缓存模块带来不便，需要继续优化
     * @param deviceId
     * @param ruleParam
     * @return
     */
    @Override
    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam) {
        // 从规则条件中取出画像标签属性条件
        HashMap<String, String> userProfileParams = ruleParam.getUserProfileParams();
        // 取出条件的中所要求的所有待查询标签名
        Set<String> tagNames = userProfileParams.keySet();
        // 构造一个hbase的查询参数对象
        Get get = new Get(deviceId.getBytes());
        // 把要查询的标签（HBASE中的列）逐一添加到get参数中
        for (String tagName : tagNames) {
            // 拿到一个标签就放到get查询中
            get.addColumn("f".getBytes(), tagName.getBytes());
        }
        // 调用hbase的api执行查询，不能将异常抛给上层
        try {
            Result result = table.get(get);
            // 判断结果与条件中要求是否一致
            for (String tagName : tagNames) {
                // 从查询结果中取出该标签的值
                byte[] valueBytes = result.getValue("f".getBytes(), tagName.getBytes());
                // 判断查询到的value和条件中要求的value是否一致，如果不一致，方法直接返回：false
                if (!(valueBytes != null && new String(valueBytes).equals(userProfileParams.get(tagName)))) {
                    return false;
                }
            }
            // 如果循环走完了，那说明每个标签的查询值都等于条件中要求的值，则可以返回true
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 如果运行到这个地方，说明前面的查询出现的了异常，返回false
        return false;
    }
}
