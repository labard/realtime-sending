package ru.at_consulting.dmp.ignite.cacheLoader;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import ru.at_consulting.dmp.ignite.*;

import java.util.Map;

public class PutFileSystemMain {

    public static void main(String[] arguments) throws Exception {
        final UtilsFilesystem utils = new UtilsFilesystem();
        //создаём две таблицы: внешнюю таблицу для временного хранения данных
        // и таблицу постоянного хранения внутри hive в формате orc
        UtilsFilesystem.FILE_SYSTEM_WRITER.createTables();
        try (final Ignite ignite = Ignition.start("ignite_FileSystem.xml")) {
            IgniteCache<RealtimeSending.Key, RealtimeSending> cache = ignite.getOrCreateCache("RealtimeSendings");
            for (int i = 0; i < 10; i++) {
                //генерируем и кладём в кэш данные.
                final Map<RealtimeSending.Key, RealtimeSending> data = UtilsFilesystem.getData(1000000);
                cache.putAll(data);
                //дожидаемся окончания записи в hadoop через файловую систему
                // и соответственно во внешнюю таблицу(если метод write all выполняется синхронно
                // при работе кластера- можно убрать latch)
                UtilsFilesystem.FILE_SYSTEM_WRITER.getLatch().await();
                //перемещаем данные из внешней таблицы в таблицу постоянного хранения внутри hive, в процессе,
                //средствами hive, осуществляется конвертация в orc
                utils.moveData();
                //очищаем папку в hadoop и соответсвенно внешнюю таблицу
                UtilsFilesystem.FILE_SYSTEM_WRITER.cleanDir();
            }
        }
    }
}
