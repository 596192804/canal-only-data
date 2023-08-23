package com.alibaba.otter.canal.connector.kafka.util;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.*;

/**
 * The type Clickhouse client.
 *
 * @Author XieChuangJian
 * @Date 2021 /11/24
 */
public class ClickHouseClient {
    private static volatile DruidDataSource dataSource;


    /**
     * @return DruidDataSource
     * @Author XieChuangJian
     * @Description Druid数据源，若未初始化，则采用双重校验锁（DCL）方式创建实例
     * @Date 2023/6/16
     */
    public static DruidDataSource getDataSourceInstance(String url, String username, String password) throws SQLException {
        if (dataSource == null) {
            synchronized (ClickHouseClient.class) {
                if (dataSource == null) {
                    DruidDataSource tmp = new DruidDataSource();
                    tmp.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
                    tmp.setUrl(url);
                    tmp.setUsername(username);
                    tmp.setPassword(password);
                    tmp.setValidationQuery("SELECT 1");
                    tmp.setTestWhileIdle(true);
                    tmp.setMaxActive(30);     //最大连接数量
                    tmp.setLogAbandoned(false);
                    tmp.init();
                    dataSource = tmp;
                }
            }
        }
        return dataSource;
    }

    /**
     * Execute sql.
     *
     * @param sql        要执行的SQL语句
     * @param connection the connection
     * @throws SQLException the sql exception
     * @Author XieChuangJian
     * @Description 调用ClickhouseJDBC客户端访问Clickhouse并执行SQL
     * @Date 2021 /11/24
     */
    public static void executeSQL(String sql, Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery(sql);
        }
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
        return !isEmpty(checkSQL, connection);
    }

    /**
     * @param checkSQL   要检测的SQL
     * @param connection 数据库连接
     * @return boolean
     * @Author XieChuangJian
     * @Description 判断执行SQL后结果是否为空
     * @Date 2023/6/13
     */
    public static boolean isEmpty(String checkSQL, Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(checkSQL);
            return !rs.next();
        }
    }

    private ClickHouseClient() {
        throw new IllegalStateException("Utility class");
    }
}
