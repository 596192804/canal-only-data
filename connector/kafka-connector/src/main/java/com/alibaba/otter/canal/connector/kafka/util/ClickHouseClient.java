package com.alibaba.otter.canal.connector.kafka.util;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.*;

/**
 * @Author XieChuangJian
 * @Date 2021/11/24
 */
public class ClickHouseClient {
    public static DruidDataSource dataSource = null;
    public static Connection connection = null;

    public static void init(String url, String username, String password) throws SQLException {
        if (dataSource == null) {
            dataSource = new DruidDataSource();
            dataSource.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setValidationQuery("SELECT 1");
            dataSource.setTestWhileIdle(true);
        }
        dataSource.init();
        connection = dataSource.getConnection();
    }

    /**
     * @param sql 要执行的SQL语句
     * @return java.sql.ResultSet
     * @Author XieChuangJian
     * @Description 调用ClickhouseJDBC客户端访问Clickhouse并执行SQL
     * @Date 2021/11/24
     */
    public static ResultSet executeSQL(String sql) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }
}
