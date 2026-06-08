package tech.ydb.spark.connector.write;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;

class YdbWriterDataQuery extends YdbWriterProtobuf {
    private final String query;
    private final ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();

    YdbWriterDataQuery(String command, String tablePath, YdbTypes types, int maxRowsCount, int maxBytesSize,
            List<ColumnEntry> columns) {
        super(types, columns, maxRowsCount, maxBytesSize);

        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        sb.append(columns.stream()
                .map(c -> "`" + c.getName() + "`:" + c.getType())
                .collect(Collectors.joining(",")));
        sb.append(">>;\n");
        sb.append(command)
                .append(" INTO `")
                .append(tablePath)
                .append("`  SELECT * FROM AS_TABLE($input);");

        this.query = sb.toString();
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
                Params prms = Params.of("$input", data);
                return session.executeDataQuery(query, TxControl.serializableRw(), prms, settings)
                        .thenApply(Result::getStatus);
            }
        };
    }
}
