package com.feelings.marketing.connector.functions;

import com.feelings.marketing.connector.pojo.GaoDeStation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author: sodamnsure
 * @Date: 2021/6/18 5:28 下午
 * @desc: 自定义MySQL Source
 * MysqlSource 的执行顺序
 * 1. 调用 MysqlSource 类的构造方法（没有提供构造方法）
 * 2. 调用open方法，仅会调用一次
 * 3. 调用run方法，会一直不停的产生数据
 * 4. 调用cancel方法可以退出run方法
 * 5. 调动close方法，释放资源
 */
public class MysqlSource extends RichParallelSourceFunction<GaoDeStation> {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet result = null;
    private boolean flag = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.21.7.6:3306/fp_support";
        String userName = "bd_write";
        String passWord = "1dVY#DNOSn3@gGzE";
        Class.forName(driver);
        conn = DriverManager.getConnection(url, userName, passWord);
        String sql = "select id,avgTicketPrice,cancelled,carrier,dest,destAirportID,origin,originAirportID from flight";
        ps = conn.prepareStatement(sql);

        super.open(parameters);
    }

    @Override
    public void run(SourceContext<GaoDeStation> sourceContext) throws Exception {


    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
