package ru.at_consulting.dmp.hdfs;

import ru.at_consulting.dmp.ignite.Utils;

import java.io.IOException;
import java.sql.*;

/**
 * Created by DAIvanov on 25.08.2016.
 */
public class JDBCtest {
    public static void main(String[] args) throws IOException, SQLException {
        final JDBCtest test = new JDBCtest();
        test.createTable();
        test.insert();
    }

    private void createTable() throws SQLException {
        String url = Utils.HIVE_URL+"/"+Utils.DB_NAME;
        try (final Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS TEST_JDBC\n" +
                    "(\n" +
                    "  MSISDN INT,\n" +
                    "  CAMPAIGN_ID INT,\n" +
                    "  SMS_TYPE STRING,\n" +
                    "  STARTTIME STRING,\n" +
                    "  SUBS_KEY BIGINT,\n" +
                    "  SEND_DT STRING,\n" +
                    "  DELIVERY_DT STRING,\n" +
                    "  START_SEND_DT STRING,\n" +
                    "  STATUS_ID INT,\n" +
                    "  RESULT_ID BIGINT,\n" +
                    "  ERROR_TEXT STRING,\n" +
                    "  MAX_TIME_OFFSET BIGINT\n" +
                    ")" +
                    "  STORED AS ORC");
        }
    }

    private void insert() throws IOException {
        String url = Utils.HIVE_URL+"/"+Utils.DB_NAME;;
        try (
                final Connection connection = DriverManager.getConnection(url);
                final Statement statement = connection.createStatement();
        ) {
            for (int i = 0; i < 30; i++) {
                String sql = "INSERT INTO R ";
                sql += setFirstParameter();
                for (int j = 0; j < 999; j++) {
                    sql += ",";
                    sql += setParameters();
                }
                long start = System.currentTimeMillis();
                statement.execute(sql);
                System.err.println("Data load: " + (System.currentTimeMillis() - start));
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private String setFirstParameter() throws SQLException {
        StringBuilder result = new StringBuilder("VALUES");
        result.append(setParameters());
        return result.toString();
    }

    private String setParameters() throws SQLException {
        final int base = (int) (Math.random() * 10000000);
        StringBuilder result = new StringBuilder("(");
        result.append(base).append(",");
        result.append(base).append(",");
        result.append("\'").append("SMS_TYPE").append(base).append("\',");
        result.append("\'").append(new Timestamp(System.currentTimeMillis()).toString()).append("\',");
        result.append(base).append(1).append(",");
        result.append("\'").append(new Timestamp(System.currentTimeMillis()).toString()).append("\',");
        result.append("\'").append(new Timestamp(System.currentTimeMillis()).toString()).append("\',");
        result.append("\'").append(new Timestamp(System.currentTimeMillis()).toString()).append("\',");
        result.append(base).append(",");
        result.append(base).append(2).append(",");
        result.append("\'").append("ERROR_TEXT").append(base).append("\',");
        result.append(base).append(3);
        result.append(")");
        return result.toString();
    }
}
