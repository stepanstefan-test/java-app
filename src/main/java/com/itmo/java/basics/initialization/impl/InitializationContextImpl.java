package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.initialization.TableInitializationContext;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level= AccessLevel.PRIVATE, makeFinal = true)
public class InitializationContextImpl implements InitializationContext {

    ExecutionEnvironment executionEnvironment;
    DatabaseInitializationContext currentDatabaseContext;
    TableInitializationContext currentTableContext;
    SegmentInitializationContext currentSegmentContext;

    @Builder
    public InitializationContextImpl(ExecutionEnvironment executionEnvironment,
                                     DatabaseInitializationContext currentDatabaseContext,
                                     TableInitializationContext currentTableContext,
                                     SegmentInitializationContext currentSegmentContext) {
        this.executionEnvironment = executionEnvironment;
        this.currentDatabaseContext = currentDatabaseContext;
        this.currentTableContext = currentTableContext;
        this.currentSegmentContext = currentSegmentContext;
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        return currentDatabaseContext;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        return currentTableContext;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        return currentSegmentContext;
    }
}
