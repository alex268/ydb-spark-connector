package tech.ydb.spark.connector.write;

import java.util.List;
import java.util.stream.Collectors;

import tech.ydb.core.Result;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;

class YdbWriterDataQuery extends YdbWriterProtobuf {
    private final String query;
    private final ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();

    YdbWriterDataQuery(String command, String tablePath, YdbTypes types, List<ColumnEntry> columns) {
        super(types, columns);

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
    protected Task buildTask(ListValue data) {
        Params prms = Params.of("$input", data);
        return session -> session.executeDataQuery(query, TxControl.serializableRw(), prms, settings)
                .thenApply(Result::getStatus);
    }
}
