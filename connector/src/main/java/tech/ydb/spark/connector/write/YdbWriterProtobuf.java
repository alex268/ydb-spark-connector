package tech.ydb.spark.connector.write;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

abstract class YdbWriterProtobuf implements YdbWriter {
    private final YdbTypes types;
    private final StructType structType;
    private final ColumnEntry[] columns;
    private final int maxRowsCount;
    private final int maxBytesSize;

    private List<Value<?>> batch = new ArrayList<>();
    private int bytesSize = 0;

    protected YdbWriterProtobuf(YdbTypes types, List<ColumnEntry> columnsList, int maxRowsCount, int maxBytesSize) {
        this.types = types;
        this.maxRowsCount = maxRowsCount;
        this.maxBytesSize = maxBytesSize;

        Map<String, Type> structTypes = new HashMap<>();
        Map<String, ColumnEntry> structColumns = new HashMap<>();
        for (ColumnEntry col : columnsList) {
            structTypes.put(col.getName(), col.getType());
            structColumns.put(col.getName(), col);
        }

        this.structType = StructType.of(structTypes);
        this.columns = new ColumnEntry[structType.getMembersCount()];
        for (int idx = 0; idx < structType.getMembersCount(); idx += 1) {
            this.columns[idx] = structColumns.get(structType.getMemberName(idx));
        }
    }

    @Override
    public boolean needToFlush() {
        return bytesSize >= maxBytesSize || batch.size() >= maxRowsCount;
    }

    @Override
    public void appendRow(InternalRow record) {
        Value<?>[] row = new Value<?>[columns.length];
        for (int idx = 0; idx < row.length; ++idx) {
            row[idx] = columns[idx].read(types, record);
            bytesSize += row[idx].toPb().getSerializedSize();
        }
        batch.add(structType.newValueUnsafe(row));
    }

    @Override
    public Batch buildNextBatch() {
        Value<?>[] copy = batch.toArray(new Value<?>[0]);
        batch = new ArrayList<>();
        bytesSize = 0;
        return buildTask(ListValue.of(copy));
    }

    protected abstract Batch buildTask(ListValue data);

    @Override
    public void close() {
    }
}
