package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.index.impl.EnvironmentIndex;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {

    DatabaseConfig config;
    EnvironmentIndex databases = new EnvironmentIndex();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return databases.searchForKey(name);
    }

    @Override
    public void addDatabase(Database db) {
        databases.onIndexedEntityUpdated(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(config.getWorkingPath());
    }
}
