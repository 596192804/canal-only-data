import com.alibaba.otter.canal.connector.kafka.util.ClickHouseClient;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author XieChuangJian
 * @Date 2023/8/23
 */
public class TestClickhouseClient {
    @Test
    public void test() {
        try (Connection connection =ClickHouseClient.getDataSourceInstance("jdbc:clickhouse://192.168.50.2:8123/default",
                "default",
                "").getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show databases;");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
            statement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        ;
    }
}
