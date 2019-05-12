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
import java.util.stream.Stream;

public class MyDAO implements DAO {
    private static final String BASE_NAME = "SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final int MAX_TABLES = 8;

    private final long flushThreshold;
    private final File base;
    private final Table memTable = new MemTable();
    private final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
    private final List<FileTable> fileTables;
    private int generation;
    private boolean errorOccurred;

    /**
     * The Log-Structured Merge-Tree implementation DAO.
     * @param base path to working directory
     * @param flushThreshold threshold value of flush
     * @throws IOException if walk on base directory is failed or can`t create SSTable
     */
    public MyDAO(
            final File base,
            final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        this.generation = 0;
        this.fileTables = new ArrayList<>();

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        addFileTable(p.toFile());
                        generation = Math.max(generation, getGenerationOf(p.getFileName().toString()));
                    });
        }

        if (errorOccurred) {
            throw new IOException("can`t create FileTable");
        }
    }

    private void addFileTable(final File file) {
        try {
            fileTables.add(new FileTable(file));
        } catch (IOException e) {
            errorOccurred = true;
        }
    }

    /**
     * Iterator for only alive cells.
     * @param from value of key of started position iterator
     * @return Iterator with alive cells
     * @throws IOException if memTable.iterator(from) is failed
     */
    private Iterator<Cell> iteratorAliveCells(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> listIterators = new ArrayList<>();
        for (final FileTable fileTable : fileTables) {
            listIterators.add(fileTable.iterator(from));
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

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        checkThreshold();
    }

    private void checkThreshold() throws IOException{
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
        if (fileTables.size() > MAX_TABLES) {
            compact();
        }
    }

    private void flush() throws IOException {
        generation++;
        final File tmp = new File(base, generation + BASE_NAME + TEMP);
        FileTable.write(memTable.iterator(emptyBuffer), tmp);
        final File dest = new File(base, generation + BASE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        memTable.clear();
        fileTables.add(new FileTable(dest));
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> cellIterator = iteratorAliveCells(emptyBuffer);
        generation++;
        final File tmp = new File(base, generation + BASE_NAME + TEMP);
        FileTable.write(cellIterator, tmp);
        final File dest = new File(base, generation + BASE_NAME + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        memTable.clear();

        for (final FileTable fileTable : fileTables) {
            fileTable.close();
        }
        fileTables.clear();

        try (Stream<Path> files = Files.walk(base.toPath())) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(BASE_NAME + SUFFIX))
                    .forEach(p -> {
                        if (getGenerationOf(p.getFileName().toString()) != generation) {
                            deleteFile(p);
                        }
                    });
        }

        if (errorOccurred) {
            throw new IOException("Can not delete file");
        }

        fileTables.add(new FileTable(dest));
    }

    private void deleteFile(final Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            errorOccurred = true;
        }
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
        for (final FileTable fileTable : fileTables) {
            fileTable.close();
        }
        fileTables.clear();
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