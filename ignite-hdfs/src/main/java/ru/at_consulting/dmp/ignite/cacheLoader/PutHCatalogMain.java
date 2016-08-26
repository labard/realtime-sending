package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ru.at_consulting.dmp.ignite.HCatalogWriter;
import ru.at_consulting.dmp.ignite.RealtimeSending;
import ru.at_consulting.dmp.ignite.Utils;

import java.util.Map;

public class PutHCatalogMain {

    public static void main(String[] arguments) throws Exception {
        HCatalogWriter.createTable();
        try (final Ignite ignite = Ignition.start("ignite_HCatalog.xml")) {
            IgniteCache<RealtimeSending.Key, RealtimeSending> cache = ignite.getOrCreateCache("RealtimeSendings");
            for (int i = 0; i < 10; i++) {
                final Map<RealtimeSending.Key, RealtimeSending> data = Utils.getData(10000);
                cache.putAll(data);
            }
        }
    }
}
