package com.alibaba.otter.canal.connector.kafka.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.*;

/**
 * The type Click house client.
 *
 * @Author XieChuangJian
 * @Date 2021 /11/24
 */
public class ClickHouseClient {
    public static DruidDataSource dataSource;

    /**
     * @Author XieChuangJian
     * @Description 初始化Druid数据源
     * @Date 2022/4/19
     * @param url Clickhouse的URL
     * @param username Clickhouse的用户名
     * @param password Clickhouse的密码
     */
    public static void init(String url, String username, String password) throws SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTestWhileIdle(true);
        dataSource.setMaxActive(30000);
        dataSource.setRemoveAbandoned(true);        //定期清除废弃链接
        dataSource.setRemoveAbandonedTimeout(120);
        dataSource.setLogAbandoned(false);
        dataSource.init();
    }

    /**
     * Execute sql.
     *
     * @param sql        要执行的SQL语句
     * @param connection the connection
     * @return java.sql.ResultSet
     * @throws SQLException the sql exception
     * @Author XieChuangJian
     * @Description 调用ClickhouseJDBC客户端访问Clickhouse并执行SQL
     * @Date 2021 /11/24
     */
    public static void executeSQL(String sql, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeQuery(sql);
        statement.close();
    }

    /**
     * @param database   库名
     * @param table      表名
     * @param connection jdbc连接
     * @return boolean
     * @Author XieChuangJian
     * @Description 判断CK表是否存在
     * @Date 2022/3/2
     */
    public static boolean isExist(String database, String table, Connection connection) throws SQLException {
        String checkSQL = "show tables in " + database + " like '" + table + "';";
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(checkSQL);
        boolean isExist = rs.next();
        statement.close();
        return isExist;
    }

    public static boolean isEmpty(String checkSQL, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(checkSQL);
        boolean isExist = rs.next();
        statement.close();
        return !isExist;
    }
}
