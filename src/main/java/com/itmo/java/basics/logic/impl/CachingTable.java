package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        cache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var value = cache.get(objectKey);

        return value == null
                ? table.read(objectKey)
                : Optional.of(value);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }
}
