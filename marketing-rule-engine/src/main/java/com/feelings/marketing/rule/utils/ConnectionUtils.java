package com.feelings.marketing.rule.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/24 4:13 下午
 * @desc: 各类外部链接创建工具类
 */
public class ConnectionUtils {
    public static Connection getClickHouseConnection() throws Exception {
        String ckDriver = "com.github.housepower.jdbc.ClickHouseDriver";
        String ckUrl = "jdbc:clickhouse://feelings:9000/default";
        String table = "event_detail";

        Class.forName(ckDriver);
        return DriverManager.getConnection(ckUrl);
    }
}
