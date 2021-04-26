package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {

    public static final int MAX_SEGMENT_SIZE = 100_000;

    private final String segmentName;
    private final Path path;
    private long segmentSize = 0;
    private boolean isReadOnly = false;
    private final SegmentIndex indexes;

    private SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.path = tableRootPath.resolve(segmentName);
        indexes = new SegmentIndex();
    }

    private SegmentImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index, boolean isReadOnly) {
        this.segmentName = segmentName;
        this.path = segmentPath;
        this.segmentSize = currentSize;
        this.indexes = index;
        this.isReadOnly = isReadOnly;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        if (segmentName == null || tableRootPath == null)
            throw new DatabaseException("Segment name or table root path is null");

        SegmentImpl segment = new SegmentImpl(segmentName, tableRootPath);

        try {
            Files.createFile(segment.path);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return segment;
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {

        return new SegmentImpl(
                context.getSegmentName(),
                context.getSegmentPath(),
                context.getCurrentSize(),
                context.getIndex(),
                context.getCurrentSize() >= MAX_SEGMENT_SIZE
        );
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private boolean writeToFile(WritableDatabaseRecord record) throws IOException {

        if (isReadOnly || record.getKey() == null)
            return false;

        try (DatabaseOutputStream outputStream = new DatabaseOutputStream(
                new FileOutputStream(
                        String.valueOf(path), true
                )
        )) {

            var segmentOffset = outputStream.write(record);

            indexes.onIndexedEntityUpdated(new String(record.getKey()), new SegmentOffsetInfoImpl(segmentSize));
            segmentSize += segmentOffset;
        }

        if (segmentSize >= MAX_SEGMENT_SIZE) isReadOnly = true;

        return true;
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        return writeToFile(
                objectValue != null
                        ? new SetDatabaseRecord(
                        objectKey.getBytes(StandardCharsets.UTF_8), objectValue
                )
                        : new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        var key = indexes.searchForKey(objectKey);
        Optional<DatabaseRecord> value;

        if (objectKey == null || key.isEmpty())
            return Optional.empty();

        try (DatabaseInputStream in = new DatabaseInputStream(
                new FileInputStream(
                        String.valueOf(path)
                )
        )) {

            in.skip(key.get().getOffset());
            value = in.readDbUnit();
        }

        return value.get().isValuePresented()
                ? Optional.of(value.get().getValue())
                : Optional.empty();
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {

        return writeToFile(new RemoveDatabaseRecord(
                objectKey.getBytes(StandardCharsets.UTF_8)));
    }
}
