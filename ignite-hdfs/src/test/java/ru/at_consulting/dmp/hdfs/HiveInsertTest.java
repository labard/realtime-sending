package ru.at_consulting.dmp.hdfs;

import java.io.IOException;
import java.sql.*;

/**
 * Created by DAIvanov on 18.08.2016.
 */
public class HiveInsertTest {
    public static void main(String[] args) throws IOException, SQLException {
        final HiveInsertTest loadMain = new HiveInsertTest();
   //     loadMain.createTable();
        loadMain.insert();
    }

    private void createTable() throws SQLException {
        String url = "jdbc:hive2://";
        try (final Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE REALTIME_SENDINGS\n" +
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
                    ")");
        }
    }

    private void insert() throws IOException {
        String url = "jdbc:hive2:///;?hive.execution.engine=tez";
        //       String sql = IOUtils.toString(getClass().getResourceAsStream("Insert.dll"), StandardCharsets.UTF_8);
        //  String url = "jdbc:hive2://dmp-hdp-1:10000/sending;hive.server2.logging.operation.enabled=true,hive.server2.logging.operation.verbose=true";
        try (
                final Connection connection = DriverManager.getConnection(url);
                final PreparedStatement preparedStatement = connection.prepareStatement("INSERT\n" +
                        "  INTO REALTIME_SENDINGS\n" +
                        "VALUES (? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        ) {
            for (int i = 0; i < 100; i++) {
                setParameters(preparedStatement);
                preparedStatement.execute();
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private void setParameters(PreparedStatement statement) throws SQLException {
        final int base = (int) (Math.random() * 10000000);
        statement.setInt(1, base);
        statement.setInt(2, base);
        statement.setString(3, "SMS_TYPE" + base);
        statement.setString(4, new Timestamp(System.currentTimeMillis()).toString());
        statement.setLong(5, base + 1);
        statement.setString(6, new Timestamp(System.currentTimeMillis()).toString());
        statement.setString(7, new Timestamp(System.currentTimeMillis()).toString());
        statement.setString(8, new Timestamp(System.currentTimeMillis()).toString());
        statement.setInt(9, base);
        statement.setLong(10, base + 2);
        statement.setString(11, "ERROR_TEXT" + base);
        statement.setLong(12, base + 3);
    }

    private void readTable() throws SQLException {
        String url = "jdbc:hive2://";
        try (final Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            final ResultSet resultSet = statement.executeQuery("SELECT * FROM REALTIME_SENDINGS");
            while (resultSet.next()) {
                System.err.println(resultSet.getInt(1));
            }
        }
    }

}
