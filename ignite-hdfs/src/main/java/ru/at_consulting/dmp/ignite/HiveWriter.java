package ru.at_consulting.dmp.ignite;

import java.util.List;


public interface HiveWriter<E> {
    void write(List<E> data) throws Exception;
}
