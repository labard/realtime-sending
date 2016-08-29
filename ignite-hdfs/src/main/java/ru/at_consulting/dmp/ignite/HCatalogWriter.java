package ru.at_consulting.dmp.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;


public class HCatalogWriter implements HiveWriter<String> {
    private static final Logger logger = LoggerFactory.getLogger(HCatalogWriter.class);
    private final HiveEndPoint hiveEP;
    private Integer nElementPerTransaction;
    private final HiveConf hiveConf;
    private final RecordWriter recordWriter;

    HCatalogWriter(URL hiveConfigUrl, String metastoreUri, String dbName, String tableName, @Nullable List<String> partitionVals, String delimiter, Integer nElementPerTransaction) {
        this.nElementPerTransaction = nElementPerTransaction;
        //создаём класс для записи через end point в указанную таблицу
        hiveEP = new HiveEndPoint(metastoreUri, dbName, tableName, partitionVals);
        Configuration conf = new Configuration();
        conf.addResource(hiveConfigUrl);
        hiveConf = new HiveConf(conf, Configuration.class);
        try {
            //создаём конвертер-писатель, конвертирует записи в виде строк с указанным разделителем в orc формат в соответсвии таблице,
            //используется при построении txnBatch
            recordWriter = Utils.getRecordWriterForSendings(delimiter, hiveEP);
        } catch (StreamingException | ClassNotFoundException e) {
            logger.error("Ошибка при создании RecordWriter: ", e);
            throw new IllegalStateException("Couldn't connect to hadoop",e);
        }
    }

public static void createTable() {
        Utils utils = new Utils();
        try {
            utils.createTable("TableForCatalog.ddl");
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't create table",e);
        }
    }

    public void write(List<String> delimitedData) throws InterruptedException, StreamingException {
        final int size = delimitedData.size();
        //вычисляем необходимое число транзакций
        final int nTransactions = getnTransactions(size);
        long start = System.currentTimeMillis();

        StreamingConnection connection = hiveEP.newConnection(true, hiveConf);
        //создаём батч так, чтобы он полностью умещал полученные для записи данные при указанном числе записей в транзакции
        TransactionBatch txnBatch = connection.fetchTransactionBatch(nTransactions, recordWriter);
        processTxnBatch(delimitedData, txnBatch);
        txnBatch.close();

        connection.close();

        logger.info("Data load: " + (System.currentTimeMillis() - start));
    }


    // последовательно идём по массиву данных открывая и записывая транзакции одну за другой
    private void processTxnBatch(List<String> delimitedData, TransactionBatch txnBatch) throws StreamingException, InterruptedException {
        int dataPosition = 0;
        while (txnBatch.remainingTransactions() > 0) {
            txnBatch.beginNextTransaction();
            dataPosition = writeTransaction(delimitedData, txnBatch, dataPosition);
            txnBatch.commit();
        }
    }

    private int writeTransaction(List<String> delimitedData, TransactionBatch txnBatch, int dataPosition) throws StreamingException, InterruptedException {
        int transactionPosition = 0;
        final int size = delimitedData.size();
        while (transactionPosition < nElementPerTransaction && dataPosition<size) {
            txnBatch.write(delimitedData.get(dataPosition).getBytes());
            transactionPosition++;
            dataPosition++;
        }
        return dataPosition-1;
    }

    private int getnTransactions(int size) {
        if(size<nElementPerTransaction*2){
            nElementPerTransaction = size/2;
        }
        int nTransactions = (size / nElementPerTransaction);
        if (size % nElementPerTransaction != 0) {
            nTransactions++;
        }
        return nTransactions;
    }

}
