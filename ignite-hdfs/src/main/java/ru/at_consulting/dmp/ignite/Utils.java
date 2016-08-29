package ru.at_consulting.dmp.ignite;

import org.apache.commons.io.IOUtils;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class Utils {
    public static final String HIVE_URL = APProperties.get("HIVE_URL");
    public static final String HADOOP_CONFIG_URL = APProperties.get("HADOOP_CONFIG_URL");
    public static final String META_STORE_URL = APProperties.get("META_STORE_URL");
    public static final String DB_NAME = APProperties.get("DB_NAME");

    public void createTable(String sqlPath) throws IOException {
        try (final Connection connection = DriverManager.getConnection(HIVE_URL+"/"+DB_NAME);
             PreparedStatement statement = connection.prepareStatement(getSqlFromFile(sqlPath))) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createExternalTable(String sqlPath, String dirPath) throws IOException {
        try (final Connection connection = DriverManager.getConnection(HIVE_URL+"/"+DB_NAME);
             PreparedStatement statement = connection.prepareStatement(getSqlFromFile(sqlPath))) {
            statement.setString(1, dirPath);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    String getSqlFromFile(String sql) throws IOException {
        return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(sql), StandardCharsets.UTF_8);
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
            return new HCatalogWriter(new URL(HADOOP_CONFIG_URL), META_STORE_URL, DB_NAME, tableName, null, delimiter, nElemPerTxn);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Bad config url", e);
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
