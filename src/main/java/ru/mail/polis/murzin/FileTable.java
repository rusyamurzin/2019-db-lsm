package ru.mail.polis.murzin;

import org.jetbrains.annotations.NotNull;

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

public class FileTable implements Table {
    private int rows;
    private LongBuffer offsets;
    private final File file;

    /**
    * Sorted String Table, which use FileChannel for read and write operations.
     *
     * @param file of this table
     */
    public FileTable(final File file) {
        this.file = file;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final long fileSize = file.length();
            assert fileSize <= Integer.MAX_VALUE;

            // Rows
            long offset = fc.size() - Long.BYTES;
            final long rowsValue = readLong(fc, offset);
            assert rowsValue <= Integer.MAX_VALUE;
            this.rows = (int) rowsValue;

            // Offsets
            offset -= (long) Long.BYTES * rows;
            this.offsets = readBuffer(fc, offset, Long.BYTES * rows).asLongBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void write(final Iterator<Cell> cellsIterator, final File to) {
        try (FileChannel fc = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FileChannel openReadFileChannel() {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            final long offset = offsets.get(i);
            assert offset <= Integer.MAX_VALUE;

            final int keySize = readInt(fc, (int) offset);

            return readBuffer(fc, offset + Integer.BYTES, keySize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        try (FileChannel fc = openReadFileChannel()) {
            assert fc != null;
            long offset = offsets.get(i);
            assert offset <= Integer.MAX_VALUE;

            // Key
            final int keySize = readInt(fc, (int) offset);
            offset += Integer.BYTES;
            final ByteBuffer key = readBuffer(fc, offset, keySize);
            offset += keySize;

            // Timestamp
            final long timeStamp = readLong(fc, (int) offset);
            offset += Long.BYTES;

            if (timeStamp < 0) {
                return new Cell(key.slice(), new Value(-timeStamp, null));
            } else {
                final int valueSize = readInt(fc, (int) offset);
                offset += Integer.BYTES;
                final ByteBuffer value = readBuffer(fc, offset, valueSize);
                return new Cell(key, new Value(timeStamp, value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int position(final ByteBuffer from) {
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

    private long readLong(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    private int readInt(final FileChannel fc, final long offset) {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        try {
            fc.read(buffer, offset);
            return buffer.rewind().getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private ByteBuffer readBuffer(final FileChannel fc, final long offset, final int size) {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        try {
            fc.read(buffer, offset);
            return buffer.rewind();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    @Override
    public long sizeInBytes() {
        return 0;
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
                return cellAt(next++);
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
    public Cell get(@NotNull final ByteBuffer key) {
        final int position = position(key);
        if (position < 0 || position >= rows) {
            return null;
        }
        final Cell cell = cellAt(position);
        if (!cell.getKey().equals(key)) {
            return null;
        }
        return cell;
    }
}
