package ru.mail.polis.murzin;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FileTable implements Table, Closeable {
    private int rows;
    private LongBuffer offsets;
    private final FileChannel fileChannel;

    /**
     * Sorted String Table, which use FileChannel for read and write operations.
     * @param file of this table
     * @throws IOException if FileChannel manipulations is failed
     */
    FileTable(final File file) throws IOException {
        this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long fileSize = file.length();
        assert fileSize <= Integer.MAX_VALUE;

        // Rows
        long offset = fileChannel.size() - Long.BYTES;
        final long rowsValue = readLong(fileChannel, offset);
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offsets
        offset -= (long) Long.BYTES * rows;
        this.offsets = readBuffer(fileChannel, offset, Long.BYTES * rows).asLongBuffer();
    }

    static void write(final Iterator<Cell> cellsIterator, final File to) throws IOException {
        FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        final List<Long> listOffsets = new ArrayList<>();
        long offset = 0;
        while (cellsIterator.hasNext()) {
            listOffsets.add(offset);

            final Cell cell = cellsIterator.next();

            // Key
            final ByteBuffer key = cell.getKey();
            final int keySize = cell.getKey().remaining();
            fc.write(Bytes.fromInt(keySize));
            offset += Integer.BYTES;
            fc.write(key);
            offset += keySize;

            // Value
            final Value value = cell.getValue();

            // Timestamp
            final long timeStamp = cell.getValue().getTimeStamp();
            fc.write(Bytes.fromLong(value.isRemoved()
                    ? -timeStamp
                    : timeStamp));
            offset += Long.BYTES;

            // Value
            if (!value.isRemoved()) {
                final ByteBuffer valueData = value.getData();
                final int valueSize = valueData.remaining();
                fc.write(Bytes.fromInt(valueSize));
                offset += Integer.BYTES;
                fc.write(valueData);
                offset += valueSize;
            }
        }
        // Offsets
        for (final Long anOffset : listOffsets) {
            fc.write(Bytes.fromLong(anOffset));
        }

        // Cells
        fc.write(Bytes.fromLong(listOffsets.size()));
        fc.close();
    }

    private ByteBuffer keyAt(final int i) throws IOException {
        assert 0 <= i && i < rows;
        assert fileChannel != null;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        final int keySize = readInt(fileChannel, (int) offset);

        return readBuffer(fileChannel, offset + Integer.BYTES, keySize);
    }

    private Cell cellAt(final int i) throws IOException {
        assert 0 <= i && i < rows;
        assert fileChannel != null;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        // Key
        final int keySize = readInt(fileChannel, (int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = readBuffer(fileChannel, offset, keySize);
        offset += keySize;

        // Timestamp
        final long timeStamp = readLong(fileChannel, (int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cell(key.slice(), new Value(-timeStamp, null));
        } else {
            final int valueSize = readInt(fileChannel, (int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = readBuffer(fileChannel, offset, valueSize);
            return new Cell(key, new Value(timeStamp, value));
        }
    }

    private int position(final ByteBuffer from) throws IOException {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + ((right - left) >> 1);
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private static long readLong(final FileChannel fc, final long offset) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        fc.read(buffer, offset);
        return buffer.rewind().getLong();
    }

    private static int readInt(final FileChannel fc, final long offset) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fc.read(buffer, offset);
        return buffer.rewind().getInt();
    }

    private static ByteBuffer readBuffer(final FileChannel fc, final long offset, final int size) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        fc.read(buffer, offset);
        return buffer.rewind();
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException("");
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<Cell>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("FileTable iterator has not next element");
                }
                try {
                    return cellAt(next++);
                } catch (IOException e) {
                    throw new NoSuchElementException("Next cell not found");
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Cell get(@NotNull final ByteBuffer key) throws IOException {
        final int position = position(key);
        if (position < 0 || position >= rows) {
            throw new NoSuchElementException("");
        }
        final Cell cell = cellAt(position);
        if (!cell.getKey().equals(key)) {
            throw new NoSuchElementException("");
        }
        return cell;
    }

    @Override
    public void close() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }
}
