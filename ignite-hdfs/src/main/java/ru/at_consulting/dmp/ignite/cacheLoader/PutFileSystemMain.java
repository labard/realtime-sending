package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ru.at_consulting.dmp.ignite.FileSystemWriter;
import ru.at_consulting.dmp.ignite.HCatalogWriter;
import ru.at_consulting.dmp.ignite.RealtimeSending;
import ru.at_consulting.dmp.ignite.Utils;

import java.util.Map;

public class PutFileSystemMain {

    public static void main(String[] arguments) throws Exception {
        Utils.FILE_SYSTEM_WRITER.createTables();
        try (final Ignite ignite = Ignition.start("ignite_FileSystem.xml")) {
            IgniteCache<RealtimeSending.Key, RealtimeSending> cache = ignite.getOrCreateCache("RealtimeSendings");
            for (int i = 0; i < 10; i++) {
                final Map<RealtimeSending.Key, RealtimeSending> data = Utils.getData(10000);
                cache.putAll(data);
                Utils.FILE_SYSTEM_WRITER.getLatch().await();
                Utils.FILE_SYSTEM_WRITER.moveData();
                Utils.FILE_SYSTEM_WRITER.cleanDir();
            }
        }
    }
}
