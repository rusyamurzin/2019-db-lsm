package ru.mail.polis.ruslan_murzin;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileTable implements Table {
    private int rows;
    private LongBuffer offsets;
    private ByteBuffer cells;

    public FileTable(final File file) {
        try (FileChannel fc = (FileChannel) Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
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

            // Cells
            this.cells = readBuffer(fc, 0, (int) offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void write(final Iterator<Cell> cells, final File to) {
        try (FileChannel fc = (FileChannel) Files.newByteChannel(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();

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
                if (value.isRemoved()) {
                    fc.write(Bytes.fromLong(-cell.getValue().getTimeStamp()));
                } else {
                    fc.write(Bytes.fromLong(cell.getValue().getTimeStamp()));
                }
                offset += Long.BYTES;

                // Value
                if (!value.isRemoved()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fc.write(Bytes.fromInt(valueSize));
                    offset += Integer.BYTES;
                    fc.write(valueData);
                    offset += valueSize;
                }
            }
            // Offsets
            for (final Long anOffset : offsets) {
                fc.write(Bytes.fromLong(anOffset));
            }

            // Cells
            fc.write(Bytes.fromLong(offsets.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = cells.getInt((int) offset);
        final ByteBuffer key = cells.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        // Key
        final int keySize = cells.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = cells.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        offset += keySize;

        // Timestamp
        final long timeStamp = cells.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cell(key.slice(), new Value(-timeStamp, null));
        } else {
            final int valueSize = cells.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize);
            return new Cell(key.slice(), new Value(timeStamp, value.slice()));
        }
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid - 1;
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
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<Cell>() {
            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
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
        return cell;
    }
}
