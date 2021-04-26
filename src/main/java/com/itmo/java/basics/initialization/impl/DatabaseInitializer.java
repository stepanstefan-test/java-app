package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;


public class DatabaseInitializer implements Initializer {

    TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        var dbPath = initialContext.currentDbContext().getDatabasePath();

        if (Files.notExists(dbPath)) {
            throw new DatabaseException("Database " + dbPath + " does not exist");
        } else {
            try {
                Files.walkFileTree(dbPath, new HashSet<>(), 1, new SimpleFileVisitor<>() {
                    @SneakyThrows
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        tableInitializer.perform(
                                InitializationContextImpl
                                        .builder()
                                        .executionEnvironment(initialContext.executionEnvironment())
                                        .currentDatabaseContext(initialContext.currentDbContext())
                                        .currentTableContext(TableInitializationContextImpl
                                                .builder()
                                                .tableName(file.getFileName().toString())
                                                .databasePath(initialContext.currentDbContext().getDatabasePath())
                                                .tableIndex(new TableIndex())
                                                .build())
                                        .currentSegmentContext(initialContext.currentSegmentContext())
                                        .build()
                        );
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new DatabaseException(dbPath + "reading error", e);
            }

            initialContext.executionEnvironment().addDatabase(
                    DatabaseImpl.initializeFromContext(
                            initialContext.currentDbContext()
                    )
            );
        }
    }
}

