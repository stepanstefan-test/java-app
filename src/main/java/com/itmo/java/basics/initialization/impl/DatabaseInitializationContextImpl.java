package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;
import lombok.Builder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;


public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private final String databaseName;
    private final Path databaseRoot;
    private final Map<String, Table> index;

    @Builder
    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.databaseName = dbName;
        this.databaseRoot = databaseRoot;
        index = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return databaseName;
    }

    @Override
    public Path getDatabasePath() {
        return databaseRoot.resolve(databaseName);
    }

    @Override
    public Map<String, Table> getTables() {
        return index;
    }

    @Override
    public void addTable(Table table) {
        index.put(table.getName(), table);
    }
}
