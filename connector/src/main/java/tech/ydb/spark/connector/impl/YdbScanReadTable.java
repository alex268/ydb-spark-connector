package tech.ydb.spark.connector.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.spark.connector.YdbFieldType;
import tech.ydb.spark.connector.YdbKeyRange;
import tech.ydb.spark.connector.YdbScanOptions;
import tech.ydb.table.Session;
import tech.ydb.table.query.ReadTablePart;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB table or index scan implementation through the ReadTable call.
 *
 * @author zinal
 */
public class YdbScanReadTable implements AutoCloseable {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbScanReadTable.class);

    private static final QueueItem END_OF_SCAN = new QueueItem(null);

    private final YdbScanOptions options;
    private final YdbKeyRange keyRange;
    private final ArrayBlockingQueue<QueueItem> queue;
    private final String tablePath;
    private List<String> outColumns;
    private int[] outIndexes;
    private Thread worker;
    private volatile State state;
    private volatile Exception firstIssue;
    private volatile Session session;
    private volatile GrpcReadStream<ReadTablePart> stream;
    private ResultSetReader current;

    public YdbScanReadTable(YdbScanOptions options, YdbKeyRange keyRange) {
        this.options = options;
        this.keyRange = keyRange;
        this.queue = new ArrayBlockingQueue<>(options.getScanQueueDepth());
        this.tablePath = options.getTablePath();
        this.state = State.CREATED;
    }

    public void prepare() {
        if (getState() != State.CREATED) {
            return;
        }

        LOG.debug("Configuring scan for table {}, range {}, columns {}, types {}",
                tablePath, keyRange, options.getKeyColumns(), options.getKeyTypes());

        // Configuring settings for the table scan.
        final ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder();
        // Add all required fields.
        outColumns = new ArrayList<>();
        scala.collection.Iterator<StructField> sfit = options.readSchema().seq().iterator();
        while (sfit.hasNext()) {
            String colname = sfit.next().name();
            rtsb.column(colname);
            outColumns.add(colname);
        }
        if (outColumns.isEmpty()) {
            // In case no fields are required, add the first field of the primary key.
            String colname = options.getKeyColumns().get(0);
            rtsb.column(colname);
            outColumns.add(colname);
        }
        outIndexes = new int[outColumns.size()];
        configureRanges(rtsb);
        if (options.getRowLimit() > 0) {
            LOG.debug("Setting row limit to {}", options.getRowLimit());
            rtsb.rowLimit(options.getRowLimit());
        }
        // TODO: add setting for the maximum scan duration.
        rtsb.withRequestTimeout(Duration.ofHours(8));

        // Create or acquire the connector object.
        YdbConnector c = options.grabConnector();
        // Obtain the session (will be a long running one).
        session = c.getTableClient().createSession(
                Duration.ofSeconds(options.getScanSessionSeconds())).join().getValue();
        try {
            // Opening the stream - which can be canceled.
            stream = session.executeReadTable(tablePath, rtsb.build());
            current = null;
            state = State.PREPARED;
            Thread t = new Thread(new Worker());
            t.setDaemon(true);
            t.setName("YdbReadTable:" + tablePath);
            t.start();
            worker = t;
        } catch (Exception ex) {
            setIssue(ex);
            LOG.warn("Failed to initiate scan for table {}", tablePath, ex);
            try {
                if (stream != null) {
                    stream.cancel();
                }
            } catch (Exception tmp) {
            }
            try {
                session.close();
            } catch (Exception tmp) {
            }
            throw new RuntimeException("Failed to initiate scan for table " + tablePath, ex);
        }
    }

    public boolean next() {
        // If we have a row block, return its rows before checking any state.
        if (current != null && current.next()) {
            return true; // we have the current row
        }        // no block, or end of rows in the block
        switch (getState()) {
            case PREPARED:
                return doNext();
            case FAILED:
                throw new RuntimeException("Scan failed.", getIssue());
            case CREATED:
                throw new IllegalStateException("Scan has not been prepared.");
            default:
                return false;
        }
    }

    private boolean doNext() {
        while (true) {
            if (current != null && current.next()) {
                return true; // have next row in the current block
            }            // end of rows or no block - need next
            QueueItem qi;
            while (true) {
                try {
                    qi = queue.take();
                    break;
                } catch (InterruptedException ix) {
                }
            }
            final Exception issue = getIssue();
            if (issue != null) {
                current = null;
                setState(State.FAILED);
                throw new RuntimeException("Scan failed.", issue);
            }
            if (qi == null || qi.reader == null) {
                current = null;
                setState(State.FINISHED);
                LOG.debug("No more blocks in queue for table {}", tablePath);
                return false;
            }
            current = qi.reader;
            // Rebuild column indexes each block, because API allows
            // the server to change the column ordering.
            if (outIndexes.length != current.getColumnCount()) {
                throw new RuntimeException("Expected columns count "
                        + outIndexes.length + ", but got " + current.getColumnCount());
            }
            for (int i = 0; i < outIndexes.length; ++i) {
                outIndexes[i] = current.getColumnIndex(outColumns.get(i));
                if (outIndexes[i] < 0) {
                    throw new RuntimeException("Lost column [" + outColumns.get(i)
                            + "] in the result set");
                }
            }
            LOG.debug("Fetched the block of {} rows from the queue for table {}",
                    current.getRowCount(), tablePath);
        }
    }

    public InternalRow get() {
        final int count = outIndexes.length;
        final ArrayList<Object> values = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            values.add(options.getTypes().convertFromYdb(current.getColumn(outIndexes[i])));
        }
        return InternalRow.fromSeq(JavaConversions.asScalaBuffer(values));
    }

    @Override
    public void close() {
        setState(State.FINISHED);
        if (stream != null) {
            try {
                stream.cancel();
            } catch (Exception tmp) {
            }
        }
        if (worker != null) {
            while (worker.isAlive()) {
                queue.clear();
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ix) {
                }
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (Exception ex) {
            }
        }
        stream = null;
        session = null;
        worker = null;
        current = null;
    }

    private synchronized void setIssue(Exception issue) {
        if (firstIssue == null) {
            firstIssue = issue;
            state = State.FAILED;
        }
    }

    private synchronized Exception getIssue() {
        return firstIssue;
    }

    private synchronized State getState() {
        return state;
    }

    private synchronized void setState(State state) {
        this.state = state;
    }

    @SuppressWarnings("unchecked")
    private TupleValue makeRange(List<Object> values) {
        final List<YdbFieldType> keyTypes = options.getKeyTypes();
        final List<Value<?>> l = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); ++i) {
            Value<?> v = options.getTypes().convertToYdb(values.get(i), keyTypes.get(i));
            if (!v.getType().getKind().equals(Type.Kind.OPTIONAL)) {
                v = v.makeOptional();
            }
            l.add(v);
        }
        return TupleValue.of(l);
    }

    private void configureRanges(ReadTableSettings.Builder rtsb) {
        final YdbKeyRange.Limit realLeft = keyRange.getFrom();
        final YdbKeyRange.Limit realRight = keyRange.getTo();
        if (!realLeft.isUnrestricted()) {
            TupleValue tv = makeRange(realLeft.getValue());
            if (realLeft.isInclusive()) {
                rtsb.fromKeyInclusive(tv);
            } else {
                rtsb.fromKeyExclusive(tv);
            }
            LOG.debug("fromKey: {} -> {}", realLeft, tv);
        }
        if (!realRight.isUnrestricted()) {
            TupleValue tv = makeRange(realRight.getValue());
            if (realRight.isInclusive()) {
                rtsb.toKeyInclusive(tv);
            } else {
                rtsb.toKeyExclusive(tv);
            }
            LOG.debug("toKey: {} -> {}", realRight, tv);
        }
    }

    private void putToQueue(QueueItem qi) {
        while (true) {
            try {
                queue.add(qi);
                break; // exit the "queue put" retry loop
            } catch (IllegalStateException ise) {
                // The unlikely case of thread interrupt should not prevent us
                // from putting an item into the queue
                try {
                    Thread.sleep(35L);
                } catch (InterruptedException ix) {
                }
            }
        }
    }

    static final class QueueItem {

        final ResultSetReader reader;

        QueueItem(ResultSetReader reader) {
            this.reader = reader;
        }
    }

    enum State {
        CREATED,
        PREPARED,
        FINISHED,
        FAILED
    }

    class Worker implements Runnable {

        @Override
        public void run() {
            LOG.debug("Started background scan for table {}, range {}", tablePath, keyRange);
            try {
                stream.start(part -> {
                    final ResultSetReader rsr = part.getResultSetReader();
                    putToQueue(new QueueItem(rsr));
                    LOG.debug("Added portion of {} rows for table {} to the queue.",
                            rsr.getRowCount(), tablePath);
                }).join().expectSuccess();
            } catch (Exception ex) {
                boolean needReport = true;
                if (ex instanceof UnexpectedResultException) {
                    UnexpectedResultException ure = (UnexpectedResultException) ex;
                    if (ure.getStatus().getCode() == StatusCode.CLIENT_CANCELLED) {
                        needReport = false;
                    }
                }
                if (needReport) {
                    LOG.warn("Background scan failed for table {}, range {}", tablePath, keyRange, ex);
                    setIssue(ex);
                }
            }
            putToQueue(END_OF_SCAN);
            LOG.debug("Completed background scan for table {}, range {}", tablePath, keyRange);
        }
    }

}
