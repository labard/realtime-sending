package ru.at_consulting.dmp.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by DAIvanov on 26.08.2016.
 */
class FileSystemWriter implements HiveWriter<String> {
    private static final Logger logger = LoggerFactory.getLogger(HCatalogWriter.class);
    private final FileSystem fileSystem;
    private final String dirPath;
    private final URI uri;
    private final String hiveUrl;

    FileSystemWriter(URL configUrl, String dirPath, String hiveUrl) {
        this.dirPath = dirPath;
        this.hiveUrl = hiveUrl;
        Configuration conf = new Configuration();
        conf.addResource(configUrl);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't get filesystem", e);
        }
        uri = fileSystem.getUri();
        createTables();
    }
    private void createTables() {
        Utils utils = new Utils();
        try {
            utils.createExternalTable("TempTable.ddl",dirPath);
            utils.createTable("OrcTableForFileLoad.ddl");
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't create tables",e);
        }
    }


    @Override
    public void write(List<String> data) throws Exception {
            cleanDir(dirPath);
            loadFile(data);
            moveData();
    }

    private void cleanDir(String dirPath) throws IOException {
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

    private void loadFile(List<String> data) throws IOException {
        long start = System.currentTimeMillis();
        int base = (int) (Math.random() * 100000);
        final String filePath = dirPath + "/tableFile" + base + ".txt";
        final String pathString = uri + filePath;
        final Path outFile = new Path(pathString);
        if (fileSystem.exists(outFile))
            System.out.println(("Output already exists"));
        OutputStream out = fileSystem.create(outFile);
        loadData(out, data);
        out.close();
        logger.info("Data load: " + (System.currentTimeMillis() - start));
    }

    private void loadData(OutputStream out, List<String> data) throws IOException {
        StringBuilder builder = new StringBuilder();
        data.forEach(s -> builder.append(s).append("\r\n"));
        out.write(builder.toString().getBytes());
    }

    private void moveData() {
        long start = System.currentTimeMillis();
        try (final Connection connection = DriverManager.getConnection(hiveUrl);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO TEST_FROM_FILESYSTEM SELECT * FROM TEMP_TABLE")) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("Data move: " + (System.currentTimeMillis() - start));
    }


}
