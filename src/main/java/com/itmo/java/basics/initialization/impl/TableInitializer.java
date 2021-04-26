package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

public class TableInitializer implements Initializer {

    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var tablePath = context.currentTableContext().getTablePath();

        if (Files.notExists(tablePath)) {
            throw new DatabaseException("Table " + tablePath + " does not exist");
        } else if (Files.isRegularFile(tablePath)) {
            throw new DatabaseException(tablePath + "is not a table file");
        } else if (!Files.isReadable(tablePath)) {
            throw new DatabaseException("Table " + tablePath + " is not readable");
        } else {
            var segments = new ArrayList<Path>();

            try {
                Files.walkFileTree(tablePath, new HashSet<>(), 1, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        segments.add(file);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new DatabaseException(tablePath + "reading error", e);
            }

            if (segments.size() > 0) {

                segments.sort(Comparator.comparing(Path::getFileName));

                for (var path : segments) {
                    segmentInitializer.perform(
                            InitializationContextImpl
                                    .builder()
                                    .executionEnvironment(context.executionEnvironment())
                                    .currentDatabaseContext(context.currentDbContext())
                                    .currentTableContext(context.currentTableContext())
                                    .currentSegmentContext(SegmentInitializationContextImpl
                                            .builder()
                                            .segmentName(path.getFileName().toString())
                                            .segmentPath(path)
                                            .currentSize(0)
                                            .index(new SegmentIndex())
                                            .build())
                                    .build()
                    );
                }
            }

            context.currentDbContext().addTable(
                    TableImpl.initializeFromContext(
                            context.currentTableContext()
                    )
            );
        }
    }
}

