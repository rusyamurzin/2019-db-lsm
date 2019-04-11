package ru.mail.polis.ruslan_murzin;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

public class MyDAO implements DAO {
    private final NavigableMap<ByteBuffer, Record> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return map.tailMap(from, true).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        map.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        map.remove(key);
    }

    @Override
    public void close() {

    }
}
