package ru.mail.polis.ruslan_murzin;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class MyDAO implements DAO {
    public static final String BASE_NAME = "SSTable";
    public static final String SUFFIX = ".dat";
    public static final String TEMP = ".tmp";

    private Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int generation;
    private List<FileTable> fileTables;

    public MyDAO(
            final File base,
            final long flushThreshold) throws IOException{
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        fileTables = new ArrayList<>();
        generation = 0;

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(SUFFIX))
                    .forEach(p -> {
                        fileTables.add(new FileTable(p.toFile()));
                        generation = Math.max(generation, getGenerationOf(p.getFileName().toString()));
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> listIterators = new ArrayList<>();
        for(FileTable fileTable : fileTables) {
            listIterators.add(fileTable.iterator(from));
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

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
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
        fileTables.add(new FileTable(dest));
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

        final ConcurrentLinkedQueue<Cell> cells = new ConcurrentLinkedQueue<>();
        final AtomicInteger counter = new AtomicInteger(0);
        fileTables.forEach(table -> new Thread(() -> {
            try {
                final Cell cell = table.get(key);

                if (cell != null) {
                    cells.add(cell);
                }
            } finally {
                counter.incrementAndGet();
            }
        }).start());

        while (counter.get() < fileTables.size()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (cells.size() == 0) {
            throw new NoSuchElementException("");
        }

        final Cell cell = Collections.min(cells, Cell.COMPARATOR);
        if (cell == null || cell.getValue().isRemoved()) {
            throw new NoSuchElementException("");
        }
        final Record record = Record.of(cell.getKey(), cell.getValue().getData());
        return record.getValue();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        checkThreshold();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private int getGenerationOf(final String name) {
        for (int i = 0; i < name.length(); i++) {
            if (!Character.isDigit(name.charAt(i))) {
                if (i == 0) {
                    return 0;
                }
                else {
                    long genLong = Long.parseLong(name.substring(0, i));
                    if (genLong > Integer.MAX_VALUE) {
                        return Integer.MAX_VALUE;
                    } else if (genLong < Integer.MIN_VALUE) {
                        return Integer.MIN_VALUE;
                    }
                    else {
                        return (int) genLong;
                    }
                }
            }
        }
        return -1;
    }
}