package tech.ydb.spark.connector.read;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.spark.connector.common.OperationOption;
import tech.ydb.table.result.ResultSetReader;

/**
 *
 * @author Aleksandr Gorshenin
 */
abstract class StreamReader implements PartitionReader<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final String[] outColumns;
    private final YdbTypes types;

    private final ArrayBlockingQueue<QueueItem> queue;

    private final AtomicLong readedRows = new AtomicLong();

    private volatile String id = null;
    private volatile long startedAt = System.currentTimeMillis();
    private volatile QueueItem currentItem = null;
    private volatile Status finishStatus = null;

    protected StreamReader(YdbTypes types, int maxQueueSize, StructType schema) {
        this.types = types;
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        this.outColumns = schema.fieldNames();
    }

    protected abstract String start();

    protected abstract void cancel();

    protected void onComplete(Status status, Throwable th) {
        long ms = System.currentTimeMillis() - startedAt;
        if (status != null) {
            if (!status.isSuccess()) {
                logger.warn("[{}] reading finished with error {}", id, status);
            }
            finishStatus = status;
        }
        if (th != null) {
            logger.error("[{}] reading finished with exception", id, th);
            finishStatus = Status.of(StatusCode.CLIENT_INTERNAL_ERROR, th);
        }
        COUNTER.decrementAndGet();
        logger.info("[{}] got {} rows in {} ms", id, readedRows.get(), ms);
    }

    protected void onNextPart(ResultSetReader reader) {

        QueueItem nextItem = new QueueItem(reader);
        try {
            while (finishStatus == null) {
                if (queue.offer(nextItem, 100, TimeUnit.MILLISECONDS)) {
                    readedRows.addAndGet(reader.getRowCount());
                    return;
                }
            }
        } catch (InterruptedException ex) {
            logger.warn("[{}] reading was interrupted", id);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean next() {
        if (id == null) {
            startedAt = System.currentTimeMillis();
            id = start();
            logger.debug("[{}] started, {} total", id, COUNTER.incrementAndGet());
        }
        while (true) {
            if (finishStatus != null) {
                finishStatus.expectSuccess("Scan failed.");
                if (currentItem == null && queue.isEmpty()) {
                    return false;
                }
            }

            if (currentItem != null && currentItem.next()) {
                return true;
            }

            try {
                currentItem = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Reading was interrupted", e);
            }
        }
    }

    @Override
    public InternalRow get() {
        if (currentItem == null) {
            throw new IllegalStateException("Nothing to read");
        }
        return currentItem.get();
    }

    @Override
    public void close() {
        if (finishStatus == null) {
            cancel();
        }
    }

    private class QueueItem {
        private final ResultSetReader reader;
        private final int[] columnIndexes;

        QueueItem(ResultSetReader reader) {
            this.reader = reader;
            this.columnIndexes = new int[outColumns.length];
            int idx = 0;
            for (String column: outColumns) {
                columnIndexes[idx++] = reader.getColumnIndex(column);
            }
        }

        public boolean next() {
            return reader.next();
        }

        public InternalRow get() {
            if (columnIndexes.length == 0) {
                return InternalRow.empty();
            }
            InternalRow row = new GenericInternalRow(columnIndexes.length);
            for (int i = 0; i < columnIndexes.length; ++i) {
                types.setRowValue(row, i, reader.getColumn(columnIndexes[i]));
            }
            return row;
        }
    }

    public static int readQueueMaxSize(CaseInsensitiveStringMap options) {
        try {
            int scanQueueDepth = OperationOption.SCAN_QUEUE_DEPTH.readInt(options, 3);
            if (scanQueueDepth < 2) {
                logger.warn("Value of {} property too low, reverting to minimum of 2.",
                        OperationOption.SCAN_QUEUE_DEPTH);
                return 2;
            }

            return scanQueueDepth;
        } catch (NumberFormatException nfe) {
            logger.warn("Illegal value of {} property, reverting to default of 3.",
                        OperationOption.SCAN_QUEUE_DEPTH, nfe);
            return 3;
        }
    }
}
