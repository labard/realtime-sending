package ru.at_consulting.dmp.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class FileSystemWriter implements HiveWriter<String> {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemWriter.class);
    private final FileSystem fileSystem;
    private final String dirPath;
    private final URI uri;
    private final CountDownLatch latch;

    FileSystemWriter(URL configUrl, String dirPath, int nWriters) {
        this.dirPath = dirPath;
        Configuration conf = new Configuration();
        conf.addResource(configUrl);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't get filesystem", e);
        }
        uri = fileSystem.getUri();
        //в случае работы в кластере предполагается, что количество записывателей соответсвует количеству узлов в кластере,
        //а запись будет закончена, когда каждый из них закончит метод write.
        latch = new CountDownLatch(nWriters);
    }


    public void createTables() {
        Utils utils = new Utils();
        try {
            utils.createExternalTable("TempTable.ddl", dirPath);
            utils.createTable("OrcTableForFileLoad.ddl");
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't create tables", e);
        }
    }


    @Override
    public void write(List<String> data) throws Exception {
        final String pathString = getFullPath();
        loadFile(data, pathString);
        latch.countDown();
    }

    @NotNull
    private String getFullPath() {
        int base = (int) (Math.random() * 100000);
        final String filePath = dirPath + "/tableFile" + base + ".txt";
        return uri + filePath;
    }
    //очищает директорию в файловой системе hadoop
    public void cleanDir() throws IOException {
        long start = System.currentTimeMillis();
        final Path rootPath = new Path(uri + dirPath);
        if (fileSystem.exists(rootPath)) {
            final FileStatus[] fileStatuses = fileSystem.listStatus(rootPath);
            final Path[] paths = FileUtil.stat2Paths(fileStatuses);
            for (Path path : paths) {
                fileSystem.delete(path, true);
            }
            logger.info("Data delete: " + (System.currentTimeMillis() - start));
        }
    }
    //записываем файл
    private void loadFile(List<String> data, String path) throws IOException {
        long start = System.currentTimeMillis();
        final Path outFile = new Path(path);
        if (fileSystem.exists(outFile))
            System.out.println(("Output already exists"));
        FSDataOutputStream out = fileSystem.create(outFile);
        loadData(out, data);
        out.hsync();
        out.close();
        logger.info("Data load: " + (System.currentTimeMillis() - start));
    }

    private void loadData(OutputStream out, List<String> data) throws IOException {
        StringBuilder builder = new StringBuilder();
        data.forEach(s -> builder.append(s).append("\r\n"));
        out.write(builder.toString().getBytes());
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
