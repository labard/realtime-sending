package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.ignite.cache.store.CacheStoreAdapter;
import ru.at_consulting.dmp.ignite.RealtimeSending;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;

/**
 * Created by DAIvanov on 26.08.2016.
 */
public class DummyAdapter extends CacheStoreAdapter<RealtimeSending.Key, RealtimeSending> implements Serializable {
    @Override
    public RealtimeSending load(RealtimeSending.Key key) throws CacheLoaderException {
        throw new UnsupportedOperationException("LOAD");
    }

    @Override
    public void write(Cache.Entry<? extends RealtimeSending.Key, ? extends RealtimeSending> entry) throws CacheWriterException {
        throw new UnsupportedOperationException("WRITE");
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        throw new UnsupportedOperationException("DELETE");
    }
}
