package ru.mail.polis.murzin;

import com.google.common.collect.Iterators;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class MyDAO implements DAO {
    public static final String BASE_NAME = "SSTable";
    public static final String SUFFIX = ".dat";
    public static final String TEMP = ".tmp";

    private final long flushThreshold;
    private final File base;
    private final Table memTable = new MemTable();
    private int generation;
    private Cell minCell;

    /**
     * The Log-Structured Merge-Tree implementation DAO.
     * @param base path to working directory
     * @param flushThreshold threshold value of flush
     */
    public MyDAO(
            final File base,
            final long flushThreshold) {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        generation = 0;

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> generation = Math.max(generation, getGenerationOf(p.getFileName().toString())));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> listIterators = new ArrayList<>();
        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        final FileTable fileTable = new FileTable(p.toFile());
                        addIteratorFrom(listIterators, fileTable, from);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        final Iterator<Cell> memIterator = memTable.iterator(from);
        listIterators.add(memIterator);
        final Iterator<Cell> cells = Iters.collapseEquals(Iterators.mergeSorted(listIterators, Cell.COMPARATOR));

        final Iterator<Cell> alive =
                Iterators.filter(
                        cells,
                        cell -> !cell.getValue().isRemoved()
                        );
        return Iterators.transform(
                alive,
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private void addIteratorFrom(final List<Iterator<Cell>> listIterators,
            final FileTable fileTable, final ByteBuffer from) {
        try {
            listIterators.add(fileTable.iterator(from));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        checkThreshold();
    }

    private void checkThreshold() throws IOException{
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File tmp = new File(base, generation + BASE_NAME + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, generation + BASE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable.clear();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        final Cell memCell = memTable.get(key);

        if (memCell != null) {
            if (memCell.getValue().isRemoved()) {
                throw new NoSuchElementException("");
            }
            return memCell.getValue().getData();
        }

        minCell = null;
        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        final FileTable fileTable = new FileTable(p.toFile());
                        final Cell cell = fileTable.get(key);
                        if (cell != null) {
                            checkMinCell(cell);
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (getMinCell() == null || getMinCell().getValue().isRemoved()) {
            throw new NoSuchElementException("");
        }
        return minCell.getValue().getData();
    }

    private void checkMinCell(final Cell cell) {
        if (minCell == null || Cell.COMPARATOR.compare(minCell, cell) > 0) {
            minCell = cell;
        }
    }

    public Cell getMinCell() {
        return minCell;
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        checkThreshold();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private int getGenerationOf(final String name) {
        int result = -1;
        for (int i = 0; i < name.length(); i++) {
            if (!Character.isDigit(name.charAt(i))) {
                if (i == 0) {
                    result = 0;
                }
                else {
                    final long genLong = Long.parseLong(name.substring(0, i));
                    if (genLong > Integer.MAX_VALUE) {
                        result = Integer.MAX_VALUE;
                    } else if (genLong < Integer.MIN_VALUE) {
                        result = Integer.MIN_VALUE;
                    }
                    else {
                        result = (int) genLong;
                    }
                }
                break;
            }
        }
        return result;
    }
}