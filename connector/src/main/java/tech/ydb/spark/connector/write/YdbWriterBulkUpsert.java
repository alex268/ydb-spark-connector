package tech.ydb.spark.connector.write;

import java.util.List;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;

class YdbWriterBulkUpsert extends YdbWriterProtobuf {
    private final String tablePath;
    private final BulkUpsertSettings settings = new BulkUpsertSettings();

    YdbWriterBulkUpsert(String tablePath, YdbTypes types, List<ColumnEntry> columns) {
        super(types, columns);
        this.tablePath = tablePath;
    }

    @Override
    protected Task buildTask(ListValue data) {
        return session -> session.executeBulkUpsert(tablePath, data, settings);
    }
}
