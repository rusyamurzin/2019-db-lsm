package ru.mail.polis.murzin;

import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    Cell get(@NotNull ByteBuffer key) throws IOException;
}