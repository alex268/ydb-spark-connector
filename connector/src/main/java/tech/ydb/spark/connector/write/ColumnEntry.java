package tech.ydb.spark.connector.write;

import java.util.UUID;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public final class ColumnEntry {
    private final String name;
    private final Type type;
    // Null if column is auto-generated
    private final DataType dataType;
    private final int ordinal;

    public ColumnEntry(String name, Type type, DataType dataType, int ordinal) {
        this.name = name;
        this.type = type;
        this.dataType = dataType;
        this.ordinal = ordinal;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public Value<?> read(YdbTypes types, InternalRow row) {
        if (dataType == null) {
            return PrimitiveValue.newText(UUID.randomUUID().toString());
        }

        Object v = row.get(ordinal, dataType);
        return types.convertToYdb(v, type);
    }
}
