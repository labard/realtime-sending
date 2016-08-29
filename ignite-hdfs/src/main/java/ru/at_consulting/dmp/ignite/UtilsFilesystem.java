package ru.at_consulting.dmp.ignite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class UtilsFilesystem extends Utils {
    private static final Logger logger = LoggerFactory.getLogger(UtilsFilesystem.class);
    public static final String DIR_NAME = APProperties.get("DIR_NAME");

    public static final FileSystemWriter FILE_SYSTEM_WRITER = getFileSystemWriter();

    private static FileSystemWriter getFileSystemWriter() {
        try {
            return new FileSystemWriter(new URL(HADOOP_CONFIG_URL), DIR_NAME,1);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Bad HIVE_URL", e);
        }
    }

    public void moveData() throws IOException {
        long start = System.currentTimeMillis();
        try (final Connection connection = DriverManager.getConnection(HIVE_URL+"/"+DB_NAME);
             PreparedStatement statement = connection.prepareStatement(getSqlFromFile("MoveData.sql"))) {
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("Data move: " + (System.currentTimeMillis() - start));
    }
}
