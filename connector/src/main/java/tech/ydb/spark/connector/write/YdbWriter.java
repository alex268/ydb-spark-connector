package tech.ydb.spark.connector.write;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.spark.sql.catalyst.InternalRow;

import tech.ydb.core.Status;
import tech.ydb.table.Session;

interface YdbWriter extends AutoCloseable {
    @FunctionalInterface
    interface Task extends Function<Session, CompletableFuture<Status>> { }

    void appendRow(InternalRow record);

    int rowsCount();

    int bytesSize();

    Task buildAndReset();

    @Override
    void close();
}
