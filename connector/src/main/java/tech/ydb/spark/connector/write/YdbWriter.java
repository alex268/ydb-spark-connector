package tech.ydb.spark.connector.write;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.InternalRow;

import tech.ydb.core.Status;
import tech.ydb.table.Session;

interface YdbWriter extends AutoCloseable {
    interface Batch extends Function<Session, CompletableFuture<Status>> {
        int rowsCount();
        int bytesSize();
    }

    void appendRow(InternalRow record);

    boolean needToFlush();

    Batch buildNextBatch();

    @Override
    void close();
}
