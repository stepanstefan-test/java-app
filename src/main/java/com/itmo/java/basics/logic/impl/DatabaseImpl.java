package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {

    private final String dbName;
    private final Path path;
    private final Map<String, Table> indexes;

    public DatabaseImpl(String dbName, Path path) {
        this.dbName = dbName;
        this.path = path;
        this.indexes = new HashMap<>();
    }

    private DatabaseImpl(String dbName, Path path, Map<String, Table> index) {
        this.dbName = dbName;
        this.path = path;
        this.indexes = index;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {

        if (dbName == null || databaseRoot == null)
            throw new DatabaseException("Database name or database root path is null");

        DatabaseImpl database = new DatabaseImpl(dbName, databaseRoot.resolve(dbName));

        try {
            Files.createDirectory(database.path);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return database;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(
                context.getDbName(),
                context.getDatabasePath(),
                context.getTables()
        );
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {

        if (tableName == null)
            throw new DatabaseException("Table name is null");

        if (!indexes.containsKey(tableName)) {
            indexes.put(tableName, TableImpl.create(tableName, path, new TableIndex()));
        } else
            throw new DatabaseException("The table already exists");
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {

        var table = indexes.get(tableName);

        if (table == null)
            throw new DatabaseException("The table does not exist");

        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {

        var table = indexes.get(tableName);

        if (table == null)
            throw new DatabaseException("The table does not exist");

        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {

        var table = indexes.get(tableName);

        if (table == null)
            throw new DatabaseException("The table does not exist");

        table.delete(objectKey);
    }
}
