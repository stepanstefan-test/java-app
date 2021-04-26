package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;


public class DatabaseServerInitializer implements Initializer {

    DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        var serverPath = context.executionEnvironment().getWorkingPath();

        if (Files.notExists(serverPath)) {
            try {
                Files.createDirectory(serverPath);
            } catch (IOException e) {
                throw new DatabaseException(serverPath + "creating error", e);
            }
        } else if (Files.isRegularFile(serverPath)) {
            throw new DatabaseException(serverPath + "is not a server file");
        } else if (!Files.isReadable(serverPath)) {
            throw new DatabaseException("Server " + serverPath + " is not readable");
        } else {
            try {
                Files.walkFileTree(serverPath, new HashSet<>(), 1, new SimpleFileVisitor<>() {
                    @SneakyThrows
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        databaseInitializer.perform(
                                InitializationContextImpl
                                        .builder()
                                        .executionEnvironment(context.executionEnvironment())
                                        .currentDatabaseContext(DatabaseInitializationContextImpl
                                                .builder()
                                                .databaseRoot(context.executionEnvironment().getWorkingPath())
                                                .dbName(file.getFileName().toString())
                                                .build())
                                        .currentTableContext(context.currentTableContext())
                                        .currentSegmentContext(context.currentSegmentContext())
                                        .build());
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new DatabaseException(serverPath + "reading error", e);
            }
        }
    }
}

