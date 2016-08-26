package ru.at_consulting.dmp.ignite;

import java.util.List;

/**
 * Created by DAIvanov on 26.08.2016.
 */
public interface HiveWriter<E> {
    void write(List<E> data) throws Exception;
}
