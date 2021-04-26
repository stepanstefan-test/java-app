package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {

    private final String tableName;
    private final Path path;
    private final TableIndex indexes;
    private Segment current;

    public TableImpl(String tableName, Path path, TableIndex indexes) {
        this.tableName = tableName;
        this.path = path;
        this.indexes = indexes;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        if (tableName == null || pathToDatabaseRoot == null || tableIndex == null) {
            throw new DatabaseException("Table name or path to database root or table index is null");
        }

        var tablePath = pathToDatabaseRoot.resolve(tableName);

        CachingTable table = new CachingTable(
                new TableImpl(tableName, tablePath, tableIndex)
        );

        try {
            Files.createDirectory(tablePath);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return table;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        var tableImpl = new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex());
        tableImpl.current = context.getCurrentSegment();
        return new CachingTable(tableImpl);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {

        if (current == null || current.isReadOnly())
            current = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path);

        try {
            if (!current.write(objectKey, objectValue))
                throw new DatabaseException("Write error");
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        indexes.onIndexedEntityUpdated(objectKey, current);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        var segment = indexes.searchForKey(objectKey);

        if (segment.isPresent()) {
            try {
                return segment.get().read(objectKey);
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
        } else
            return Optional.empty();
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

        var segment = indexes.searchForKey(objectKey);

        if (segment.isPresent()) {
            try {
                if (!segment.get().delete(objectKey))
                    throw new DatabaseException("Delete error");
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
