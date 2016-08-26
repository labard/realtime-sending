package ru.at_consulting.dmp.ignite;

import org.apache.commons.io.IOUtils;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by DAIvanov on 25.08.2016.
 */
public class Utils {

    public static final String HIVE_URL = "jdbc:hive2://192.168.233.104:10000/sending";
    public static final String DIR_NAME = "/realtimeSendingsDir";
    public static final FileSystemWriter FILE_SYSTEM_WRITER = getFileSystemWriter();

    public void createTable(String sql) throws IOException {
        try (final Connection connection = DriverManager.getConnection(HIVE_URL);
             PreparedStatement statement = connection.prepareStatement(IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sql), StandardCharsets.UTF_8))) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createExternalTable(String sql, String dirPath) throws IOException {
        try (final Connection connection = DriverManager.getConnection(HIVE_URL);
             PreparedStatement statement = connection.prepareStatement(IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sql), StandardCharsets.UTF_8))) {
            statement.setString(1, dirPath);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static String convertSending(RealtimeSending.Key key, RealtimeSending sending) {
        return String.valueOf(key.getMsisdn()) + ";" +
                key.getCampaignId() + ";" +
                key.getSmsType() + ";" +
                key.getStartTime() + ";" +
                sending.getSubsKey() + ";" +
                sending.getSendDt() + ";" +
                sending.getDeliveryDt() + ";" +
                sending.getStartSendDt() + ";" +
                sending.getStatusId() + ";" +
                sending.getResultId() + ";" +
                sending.getErrorText() + ";" +

                sending.getMaxTimeOffset() + ";";
    }

    public static RecordWriter getRecordWriterForSendings(String delimiter, HiveEndPoint endPoint) throws StreamingException, ClassNotFoundException {
        String[] names = {"msisdn",
                "campaign_id",
                "sms_type",
                "starttime",
                "subs_key",
                "send_dt",
                "delivery_dt",
                "start_send_dt",
                "status_id",
                "result_id",
                "error_text",
                "max_time_offset"};
        //названия полей должны быть строчными(маленькими) буквами
        return new DelimitedInputWriter(names, delimiter, endPoint);
    }

    public static HCatalogWriter getHCatalogWriter(String tableName, String delimiter, Integer nElemPerTxn) {
        try {
            return new HCatalogWriter(new URL("http://192.168.233.104:50070/conf"), "thrift://192.168.233.104:9083", "sending", tableName, null, delimiter, nElemPerTxn);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Bad config url", e);
        }
    }

    private static FileSystemWriter getFileSystemWriter() {
        try {
            return new FileSystemWriter(new URL("http://192.168.233.104:50070/conf"), DIR_NAME, HIVE_URL,1);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Bad HIVE_URL", e);
        }
    }

    public static Map<RealtimeSending.Key, RealtimeSending> getData(Integer size) {
        Map<RealtimeSending.Key, RealtimeSending> data = new HashMap<>();
        for (int i = 0; i < size; i++) {
            final int base = (int) (Math.random() * 10000000);
            RealtimeSending.Key key = new RealtimeSending.Key(
                    base,
                    base + 1,
                    "SMS_TYPE",
                    new Timestamp(System.currentTimeMillis())
            );
            RealtimeSending value = new RealtimeSending(
                    new Timestamp(System.currentTimeMillis()),
                    new Timestamp(System.currentTimeMillis()),
                    new Timestamp(System.currentTimeMillis()),
                    base + 2,
                    base + 3,
                    base + 4,
                    "ERROR_TEXT",
                    base + 5

            );
            data.put(key, value);
        }
        return data;
    }
}
