package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        var segmentPath = context.currentSegmentContext().getSegmentPath();

        if (Files.isDirectory(segmentPath)) {
            throw new DatabaseException(segmentPath + "is not a segment file");
        } else if (!Files.isReadable(segmentPath)) {
            throw new DatabaseException("Segment " + segmentPath + " is not readable");
        } else {
            int size = 0;
            List<String> segmentKeys = new ArrayList<>();
            Optional<DatabaseRecord> segment;

            try (DatabaseInputStream in = new DatabaseInputStream(
                    new FileInputStream(
                            String.valueOf(context.currentSegmentContext().getSegmentPath())
                    )
            )) {

                while (in.available() > 0) {

                    segment = in.readDbUnit();

                    segmentKeys.add(
                            new String(segment.get().getKey()));

                    context.currentSegmentContext().getIndex().onIndexedEntityUpdated(
                            new String(segment.get().getKey()),
                            new SegmentOffsetInfoImpl(size)
                    );

                    size += segment.get().size();
                }
            } catch (IOException e) {
                throw new DatabaseException(segmentPath + "reading error", e);
            }

            context = InitializationContextImpl
                    .builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(SegmentInitializationContextImpl
                            .builder()
                            .segmentName(context.currentSegmentContext().getSegmentName())
                            .currentSize(size)
                            .index(context.currentSegmentContext().getIndex())
                            .segmentPath(context.currentSegmentContext().getSegmentPath())
                            .build())
                    .build();

            context.currentTableContext().updateCurrentSegment(
                    SegmentImpl.initializeFromContext(
                            context.currentSegmentContext()
                    )
            );

            InitializationContext finalContext = context;
            segmentKeys.forEach(key ->
                    finalContext.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                            key,
                            finalContext.currentTableContext().getCurrentSegment()
                    ));
        }
    }
}
