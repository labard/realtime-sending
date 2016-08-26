package ru.at_consulting.dmp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;

/**
 * Created by DAIvanov on 19.08.2016.
 */
public class SimpleInsertInHadoop {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new URL("http://192.168.233.104:50070/conf"));
        FileSystem fs = FileSystem.get(conf);
        final URI uri = fs.getUri();
        final Path outFile = new Path(uri + "/dima/testFile8.txt");
        if (fs.exists(outFile))
            System.out.println(("Output already exists"));
        OutputStream out = fs.create(outFile);
        loadData(out);
        out.close();

    }

    static void loadData(OutputStream stream) throws IOException {
        final byte[] bytes = getBytes();
        long start = System.currentTimeMillis();
        stream.write(bytes);
        System.err.println("Data load: " +(System.currentTimeMillis() - start));

    }

    static byte[] getBytes() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            final int base = (int) (Math.random() * 10000000);
            result.append(base).append("|");
            result.append(base).append("|");
            result.append("SMS_TYPE").append(base).append("|");
            result.append(new Timestamp(System.currentTimeMillis()).toString()).append("|");
            result.append(base).append(1).append("|");
            result.append(new Timestamp(System.currentTimeMillis()).toString()).append("|");
            result.append(new Timestamp(System.currentTimeMillis()).toString()).append("|");
            result.append(new Timestamp(System.currentTimeMillis()).toString()).append("|");
            result.append(base).append("|");
            result.append(base).append(2).append("|");
            result.append("ERROR_TEXT").append(base);
            result.append(base).append(3).append("|");
            result.append("\r\n");
        }
        return result.toString().getBytes(Charset.forName("UTF-8"));
    }
}
