package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import ru.at_consulting.dmp.ignite.HiveWriter;
import ru.at_consulting.dmp.ignite.RealtimeSending;
import ru.at_consulting.dmp.ignite.Utils;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileSystemAdapter extends DummyAdapter {
    private final HiveWriter<String> writer = Utils.FILE_SYSTEM_WRITER;

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
