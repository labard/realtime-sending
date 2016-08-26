package ru.at_consulting.dmp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;

/**
 * Created by DAIvanov on 22.08.2016.
 */
public class IncrementThoughtHiveTable {
    private FileSystem fs = initFileSystem();

    public IncrementThoughtHiveTable() throws IOException {
    }


    private FileSystem initFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new URL("http://192.168.233.104:50070/conf"));
        return FileSystem.get(conf);
    }

    private void loadFile(String dirPath, URI uri) throws IOException {
        int base = (int)(Math.random() * 100000);
        final String filePath = dirPath + "/tableFile" + base + ".txt";
        final String pathString = uri + filePath;
        final Path outFile = new Path(pathString);
        if (fs.exists(outFile))
            System.out.println(("Output already exists"));
        OutputStream out = fs.create(outFile);
        SimpleInsertInHadoop.loadData(out);
        out.close();
    }

    public static void main(String[] args) throws IOException, SQLException {
        final IncrementThoughtHiveTable incremetThrouhtHiveTable = new IncrementThoughtHiveTable();
        String dirPath = "/testTable";
        final FileSystem fs = incremetThrouhtHiveTable.fs;
        final URI uri = fs.getUri();
        InsertWithHiveExternalTable.createOrcTable();
        InsertWithHiveExternalTable.createHiveTable(dirPath);
        for(int i =0; i <30;i++) {
            cleanDir(dirPath, fs);
            incremetThrouhtHiveTable.loadFile(dirPath, uri);
            long start = System.currentTimeMillis();
            InsertWithHiveExternalTable.moveData();
            System.err.println("Data move: "+ (System.currentTimeMillis()-start));
        }
    }

    private static void cleanDir(String dirPath, FileSystem fs) throws IOException {
        long start = System.currentTimeMillis();
        URI uri = fs.getUri();
        final FileStatus[] fileStatuses = fs.listStatus(new Path(uri + dirPath));
        final Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path path : paths) {
            fs.delete(path, true);
        }
        System.err.println("Data delete: "+ (System.currentTimeMillis()-start));
    }
}
