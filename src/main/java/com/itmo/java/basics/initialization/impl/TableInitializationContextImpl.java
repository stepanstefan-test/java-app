package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import lombok.Builder;

import java.nio.file.Path;


public class TableInitializationContextImpl implements TableInitializationContext {

    private final String tableName;
    private final Path databasePath;
    private final TableIndex index;
    private Segment currentSegment;

    @Builder
    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.index = tableIndex;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return databasePath.resolve(tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return index;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }
}
