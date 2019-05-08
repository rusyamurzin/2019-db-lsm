package ru.mail.polis.murzin;

import com.google.common.collect.Iterators;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final Value previous = map.put(key, Value.of(value));
        if (previous == null) {
            sizeInBytes += key.remaining() + value.remaining() + Long.BYTES;
        } else if (previous.isRemoved()) {
            sizeInBytes += value.remaining();
        } else {
            sizeInBytes += value.remaining() - previous.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            sizeInBytes += key.remaining() + Long.BYTES;
        } else if (!previous.isRemoved()) {
            sizeInBytes -= previous.getData().remaining();
        }
    }

    @Override
    public void clear() {
        sizeInBytes = 0;
        map.clear();
    }

    @Override
    public Cell get(@NotNull final ByteBuffer key) throws IOException {
        final Value value = map.get(key);
        return value == null
                ? null
                : new Cell(key, value);
    }
}