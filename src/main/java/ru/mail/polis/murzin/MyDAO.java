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
    private static final int MAX_TABLES = 8;

    private final long flushThreshold;
    private final File base;
    private final Table memTable = new MemTable();
    private int generation;
    private Cell minCell;
    private int countTables;

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
        countTables = 0;

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        countTables++;
                        generation = Math.max(generation, getGenerationOf(p.getFileName().toString()));
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Iterator for only alive cells.
     * @param from value of key of started position iterator
     * @return Iterator with alive cells
     * @throws IOException if memTable.iterator(from) is failed
     */
    public Iterator<Cell> iteratorAliveCells(@NotNull final ByteBuffer from) throws IOException {
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
        final Iterator<Cell> cells = Iters.collapseEquals(
                Iterators.mergeSorted(listIterators, Cell.COMPARATOR),
                Cell::getKey
        );

        return Iterators.filter(
                        cells,
                        cell -> !cell.getValue().isRemoved()
                );
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> alive = iteratorAliveCells(from);
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
        if (countTables > MAX_TABLES) {
            compact();
        }
    }

    private void flush() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)));
        countTables++;
    }

    private void flush(final Iterator<Cell> cells) throws IOException {
        generation++;
        final File tmp = new File(base, generation + BASE_NAME + TEMP);
        FileTable.write(cells, tmp);
        final File dest = new File(base, generation + BASE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        memTable.clear();
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> cellIterator = iteratorAliveCells(ByteBuffer.allocate(0));
        flush(cellIterator);
        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        if (getGenerationOf(p.getFileName().toString()) != generation) {
                            deleteFile(p);
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        countTables = 1;
    }

    private void deleteFile(final Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            throw new NoSuchElementException("minCell is " + minCell);
        }
        return getMinCell().getValue().getData();
    }

    private void checkMinCell(final Cell cell) {
        if (minCell == null || Cell.COMPARATOR.compare(cell, minCell) < 0) {
            minCell = cell;
        }
    }

    private Cell getMinCell() {
        return minCell;
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
        checkThreshold();
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() != 0) {
            flush();
        }
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