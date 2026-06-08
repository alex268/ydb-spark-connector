package tech.ydb.spark.connector.write;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Status;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;

class YdbWriterBulkUpsert extends YdbWriterProtobuf {
    private final String tablePath;
    private final BulkUpsertSettings settings = new BulkUpsertSettings();

    YdbWriterBulkUpsert(String tablePath, YdbTypes types, int maxRowsCount, int maxBytesSize, List<ColumnEntry> cols) {
        super(types, cols, maxRowsCount, maxBytesSize);
        this.tablePath = tablePath;
    }

    @Override
    protected Batch buildTask(ListValue data) {
        return new Batch() {
            @Override
            public int rowsCount() {
                return data.size();
            }

            @Override
            public int bytesSize() {
                return data.toPb().getSerializedSize();
            }

            @Override
            public CompletableFuture<Status> apply(Session session) {
                return session.executeBulkUpsert(tablePath, data, settings);
            }
        };
    }
}
