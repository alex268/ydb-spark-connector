package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 * YDB table writer: basic row writer.
 *
 * @author zinal
 */
public abstract class YdbDataWriter implements DataWriter<InternalRow> {

    static final int MAX_ROWS_COUNT = 10000;
    static final int MAX_BYTES_SIZE = 10 * 1024 * 1024;
    static final int CONCURRENCY = 2;
    static final int WRITE_RETRY_COUNT = 10;

    private static final Logger logger = LoggerFactory.getLogger(YdbDataWriter.class);

    private final SessionRetryContext retryCtx;
    private final YdbTypes types;
    private final StructType structType;
    private final ValueReader[] readers;

    private final int maxRowsCount;
    private final int maxBytesSize;
    private final int maxConcurrency;
    private final Semaphore semaphore;

    private List<Value<?>> currentBatch = new ArrayList<>();
    private int currentBatchBytesSize = 0;
    private volatile Status lastError = null;

    public YdbDataWriter(SessionRetryContext retryCtx, YdbTypes types, StructType structType, ValueReader[] readers,
            int batchRowsCount, int batchBytesSize, int batchConcurrency) {
        this.retryCtx = retryCtx;
        this.types = types;
        this.structType = structType;
        this.readers = readers;
        this.maxRowsCount = batchRowsCount;
        this.maxBytesSize = batchBytesSize;
        this.maxConcurrency = batchConcurrency;
        this.semaphore = new Semaphore(maxConcurrency);
    }

    abstract CompletableFuture<Status> executeWrite(Session session, ListValue batch);

    @Override
    public void write(InternalRow record) throws IOException {
        Status localError = lastError;
        if (localError != null) {
            logger.warn("ydb writer got error {} on write", localError);
            localError.expectSuccess("Cannot execute write");
        }

        Value<?>[] row = new Value<?>[readers.length];
        for (int idx = 0; idx < row.length; ++idx) {
            row[idx] = readers[idx].read(types, record);
            currentBatchBytesSize += row[idx].toPb().getSerializedSize();
        }

        currentBatch.add(structType.newValueUnsafe(row));
        if (currentBatch.size() >= maxRowsCount || currentBatchBytesSize >= maxBytesSize) {
            writeBatch();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        writeBatch();

        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);

        Status localError = lastError;
        if (localError != null) {
            logger.error("ydb writer got error on commit: {}", localError);
            localError.expectSuccess("cannot commit write");
        }

        // All rows have been written successfully
        return new YdbWriteCommit();
    }

    @Override
    public void abort() throws IOException {
        semaphore.acquireUninterruptibly(maxConcurrency);
        semaphore.release(maxConcurrency);
    }

    @Override
    public void close() throws IOException { }

    private void writeBatch() {
        if (currentBatch.isEmpty()) {
            currentBatchBytesSize = 0;
            return;
        }

        int batchBytesSize = currentBatchBytesSize;
        Value<?>[] copy = currentBatch.toArray(new Value<?>[0]);
        OutputMetrics metrics = TaskContext.get().taskMetrics().outputMetrics();

        currentBatchBytesSize = 0;
        currentBatch = new ArrayList<>();

        semaphore.acquireUninterruptibly();
        if (lastError != null) {
            semaphore.release();
            return;
        }

        ListValue batch = ListValue.of(copy);

        retryCtx.supplyStatus(session -> executeWrite(session, batch)).whenComplete((st, th) -> {
            if (st != null && st.isSuccess()) {
                metrics._bytesWritten().add(batchBytesSize);
                metrics._recordsWritten().add(batch.size());
            } else {
                lastError = st != null ? st : Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
            }

            semaphore.release();
        });
    }
}
