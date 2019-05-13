package ru.mail.polis.murzin;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static long lastTime;
    private static long additionalTime;

    /**
     * Value which hold data with timestamp.
     * @param ts timestamp
     * @param data stored data
     */
    public Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
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

    /**
     * Get data as buffer.
     * @return data as byte buffer
     */
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalStateException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(ts, o.ts);
    }

    public long getTimeStamp() {
        return ts;
    }

    /**
     * Get current time in nanoseconds.
     * @return current time in nanoseconds
     */
    public static long getCurrentTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != lastTime) {
            additionalTime = 0;
            lastTime = currentTime;
        }
        return currentTime * 1_000_000 + ++additionalTime;
    }
}