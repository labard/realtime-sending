package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import ru.at_consulting.dmp.ignite.*;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;

public class HCatalogAdapter extends DummyAdapter {
    private final HiveWriter<String> writer = Utils.getHCatalogWriter("TEST_FOR_HCATALOG", ";", 200);

    @IgniteInstanceResource
    protected Ignite ignite;

    // This mehtod is called whenever "putAll(...)" methods are called on IgniteCache.
    public void writeAll(Collection<Cache.Entry<? extends RealtimeSending.Key, ? extends RealtimeSending>> entries) {
        final List<String> strings = new ArrayList<>();
        entries.forEach(entry -> strings.add(Utils.convertSending(entry.getKey(), entry.getValue())));
        try {
            writer.write(strings);
        } catch (Exception e) {
            ignite.log().error("Data load error", e);
        }
    }
}
