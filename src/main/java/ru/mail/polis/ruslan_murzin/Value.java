package ru.mail.polis.ruslan_murzin;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static long lastTime;
    private static long additionalTime;

    public Value(final long ts, final ByteBuffer data) {
        assert (ts >= 0);
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(getCurrentTimeNanos(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(getCurrentTimeNanos(), null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }

    public static long getCurrentTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != lastTime) {
            additionalTime = 0;
            lastTime = currentTime;
        }
        return currentTime * 1_000_000 + ++additionalTime;
    }
}