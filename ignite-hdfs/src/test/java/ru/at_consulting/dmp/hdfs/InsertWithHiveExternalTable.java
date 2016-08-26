package ru.at_consulting.dmp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.sql.*;

/**
 * Created by DAIvanov on 22.08.2016.
 */
public class InsertWithHiveExternalTable {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new URL("http://192.168.233.104:50070/conf"));
        FileSystem fs = FileSystem.get(conf);
        final URI uri = fs.getUri();
        final String filePath = "/dima/tableFile.txt";
        final String pathString = uri + filePath;
        final Path outFile = new Path(pathString);
        if (fs.exists(outFile))
            System.out.println(("Output already exists"));
        OutputStream out = fs.create(outFile);
        SimpleInsertInHadoop.loadData(out);
        out.close();
        createHiveTable("/dima");
        createOrcTable();
        moveData();

    }

     static void createHiveTable(String dirPath) throws SQLException {
        String url = "jdbc:hive2://dmp-hdp-nmd1:10000/sending";
        try (final Connection connection = DriverManager.getConnection(url);
             PreparedStatement statement = connection.prepareStatement("CREATE EXTERNAL TABLE IF NOT EXISTS REALTIME_SENDINGS_TEMP\n" +
                     "(\n" +
                     "  MSISDN INT,\n" +
                     "  CAMPAIGN_ID INT,\n" +
                     "  SMS_TYPE STRING,\n" +
                     "  STARTTIME TIMESTAMP,\n" +
                     "  SUBS_KEY BIGINT,\n" +
                     "  SEND_DT TIMESTAMP,\n" +
                     "  DELIVERY_DT TIMESTAMP,\n" +
                     "  START_SEND_DT TIMESTAMP,\n" +
                     "  STATUS_ID INT,\n" +
                     "  RESULT_ID BIGINT,\n" +
                     "  ERROR_TEXT STRING,\n" +
                     "  MAX_TIME_OFFSET BIGINT\n" +
                     ")" +
                     " ROW FORMAT DELIMITED\n" +
                     "    FIELDS TERMINATED BY '|'\n" +
                     "    STORED AS TEXTFILE\n" +
                     "    location ?")) {
            statement.setString(1, dirPath);
            statement.execute();
        }
    }

    static void createOrcTable() {
        String url = "jdbc:hive2://dmp-hdp-nmd1:10000/sending";
        try (final Connection connection = DriverManager.getConnection(url);
             PreparedStatement statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS REALTIME_SENDINGS_ORC\n" +
                     "(\n" +
                     "  MSISDN INT,\n" +
                     "  CAMPAIGN_ID INT,\n" +
                     "  SMS_TYPE STRING,\n" +
                     "  STARTTIME TIMESTAMP,\n" +
                     "  SUBS_KEY BIGINT,\n" +
                     "  SEND_DT TIMESTAMP,\n" +
                     "  DELIVERY_DT TIMESTAMP,\n" +
                     "  START_SEND_DT TIMESTAMP,\n" +
                     "  STATUS_ID INT,\n" +
                     "  RESULT_ID BIGINT,\n" +
                     "  ERROR_TEXT STRING,\n" +
                     "  MAX_TIME_OFFSET BIGINT\n" +
                     ")" +
                     " ROW FORMAT DELIMITED\n" +
                     "    FIELDS TERMINATED BY '|'\n" +
                     "    STORED AS ORC")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

     static void moveData(){
        String url = "jdbc:hive2://dmp-hdp-nmd1:10000/sending";
        try (final Connection connection = DriverManager.getConnection(url);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO REALTIME_SENDINGS_ORC SELECT * FROM REALTIME_SENDINGS_TEMP")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
