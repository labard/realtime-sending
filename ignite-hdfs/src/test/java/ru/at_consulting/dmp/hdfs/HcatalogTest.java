package ru.at_consulting.dmp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by DAIvanov on 23.08.2016.
 */
public class HcatalogTest {
    public static int N_ELEM_PER_TRANSACTION = 100;

    public static void main(String[] args) throws InterruptedException, StreamingException, IOException, ClassNotFoundException {
        createProperSchema();
        HiveEndPoint hiveEP = new HiveEndPoint("thrift://dmp-hdp-nmd1:9083", "sending", "REALTIME_SENDINGS_ORC_NEW", null);
        Configuration conf = new Configuration();
        conf.addResource(new URL("http://192.168.233.104:50070/conf"));
        HiveConf hiveConf = new HiveConf(conf, Configuration.class);
        String[] names = {"msisdn", "campaign_id", "sms_type", "starttime", "subs_key", "send_dt", "delivery_dt", "start_send_dt",
                "status_id", "result_id", "error_text", "max_time_offset"};

        DelimitedInputWriter writer = new DelimitedInputWriter(names, ";", hiveEP);


        for (int k = 0; k < 30; k++) {

            final List<String> data = getData();
            final int size = data.size();
            int nTransactions = (size / N_ELEM_PER_TRANSACTION);
            if (size % N_ELEM_PER_TRANSACTION != 0) {
                nTransactions++;
            }
            long start = System.currentTimeMillis();
            StreamingConnection connection = hiveEP.newConnection(true, hiveConf);
            TransactionBatch txnBatch = connection.fetchTransactionBatch(nTransactions, writer);
            int dataPosition = 0;
            while (txnBatch.remainingTransactions() > 0) {
                txnBatch.beginNextTransaction();
                int transactionPosition = 0;
                while (transactionPosition < N_ELEM_PER_TRANSACTION) {
                    txnBatch.write(data.get(dataPosition).getBytes());
                    transactionPosition++;
                    dataPosition++;
                }
                txnBatch.commit();
            }
            txnBatch.close();
            System.err.println("Data load: " + (System.currentTimeMillis() - start));
            connection.close();
        }
    }

    /**
     * Таблица должна быть в формате ORC,
     * иметь tblproperties("transactional"="true"),
     * должна быть bucketed, но не sorted
     */
    private static void createProperSchema() {
        String url = "jdbc:hive2://dmp-hdp-nmd1:10000/sending";
        try (final Connection connection = DriverManager.getConnection(url);
             PreparedStatement statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS REALTIME_SENDINGS_ORC_HCATALOG\n" +
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
                     " clustered by (MSISDN) into 5 buckets  " +
                     " STORED AS ORC tblproperties(\"transactional\"=\"true\")")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static List<String> getData() {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            StringBuilder element = new StringBuilder();
            final int base = (int) (Math.random() * 10000000);
            element.append(base).append(";");
            element.append(base).append(";");
            element.append("SMS_TYPE").append(base).append(";");
            element.append(new Timestamp(System.currentTimeMillis()).toString()).append(";");
            element.append(base).append(1).append(";");
            element.append(new Timestamp(System.currentTimeMillis()).toString()).append(";");
            element.append(new Timestamp(System.currentTimeMillis()).toString()).append(";");
            element.append(new Timestamp(System.currentTimeMillis()).toString()).append(";");
            element.append(base).append(";");
            element.append(base).append(2).append(";");
            element.append("ERROR_TEXT").append(base).append(";");
            element.append(base).append(3).append(";");
            result.add(element.toString());
        }
        return result;
    }
}
