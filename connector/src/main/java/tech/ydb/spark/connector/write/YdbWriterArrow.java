package tech.ydb.spark.connector.write;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Encodes Spark rows as Apache Arrow IPC payloads and ships them via {@code BulkUpsert}.
 *
 * @author Aleksandr Gorshenin
 */
public class YdbWriterArrow implements YdbWriter {
    private final RootAllocator allocator;

    private final List<ColumnEntry> columns;
    private final String tablePath;
    private final ApacheArrowWriter arrowWriter;

    private ApacheArrowWriter.Batch batch;
    private int rowsCount = 0;
    private int bytesSize = 0;

    public YdbWriterArrow(String tablePath, List<ColumnEntry> columns) {
        this.allocator = new RootAllocator();
        this.columns = columns;
        this.tablePath = tablePath;

        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        for (ColumnEntry column: columns) {
            schema.addColumn(column.getName(), column.getType());
        }

        this.arrowWriter = schema.createWriter(allocator);
        this.batch = arrowWriter.createNewBatch(0);
    }

    @Override
    public void appendRow(InternalRow record) {
        ApacheArrowWriter.Row row = batch.writeNextRow();
        for (ColumnEntry column: columns) {
            bytesSize += writeValue(column, row, record);
        }
        rowsCount++;
    }

    @Override
    public int rowsCount() {
        return rowsCount;
    }

    @Override
    public int bytesSize() {
        return bytesSize;
    }

    @Override
    public void close() {
        arrowWriter.close();
        allocator.close();
    }

    @Override
    public Task buildAndReset() {
        try {
            ApacheArrowData data = batch.buildBatch();
            rowsCount = 0;
            bytesSize = 0;
            batch = arrowWriter.createNewBatch(0);
            return session -> session.executeBulkUpsert(tablePath, data, new BulkUpsertSettings());
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to serialize Arrow batch", ex);
        }
    }

    private static int writeValue(ColumnEntry column, ApacheArrowWriter.Row row, InternalRow record) {
        DataType dataType = column.getDataType();
        String name = column.getName();
        int ordinal = column.getOrdinal();
        Type type = column.getType();

        if (dataType == null) {
            String randomKey = UUID.randomUUID().toString();
            row.writeText(column.getName(), randomKey);
            return randomKey.length();
        }

        while (type.getKind() == Type.Kind.OPTIONAL) {
            if (record.isNullAt(ordinal)) {
                row.writeNull(name);
                return column.getDataType().defaultSize();
            }
            type = type.makeOptional();
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            DecimalType realType = (DecimalType) type;
            Decimal v = record.getDecimal(ordinal, realType.getPrecision(), realType.getScale());
            row.writeDecimal(name, realType.newValue(v.toJavaBigDecimal()));
            // YDB stores Decimal as array of 16 bytes
            return 16;
        }

        if (type.getKind() != Type.Kind.PRIMITIVE) {
            throw new IllegalArgumentException("Arrow ingestion does not support type " + type);
        }

        switch ((PrimitiveType) type) {
            case Bool:
                row.writeBool(name, record.getBoolean(ordinal));
                return 1;
            case Int8:
                row.writeInt8(name, record.getByte(ordinal));
                return 1;
            case Int16:
                row.writeInt16(name, record.getShort(ordinal));
                return 2;
            case Int32:
                row.writeInt32(name, record.getInt(ordinal));
                return 4;
            case Int64:
                row.writeInt64(name, record.getLong(ordinal));
                return 8;
            case Uint8:
                row.writeUint8(name, record.getInt(ordinal));
                return 1;
            case Uint16:
                row.writeUint16(name, record.getInt(ordinal));
                return 2;
            case Uint32:
                row.writeUint32(name, record.getLong(ordinal));
                return 4;
            case Uint64:
                row.writeUint64(name, record.getLong(ordinal));
                return 8;
            case Float:
                row.writeFloat(name, record.getFloat(ordinal));
                return 4;
            case Double:
                row.writeDouble(name, record.getDouble(ordinal));
                return 4;
            case Text:
                UTF8String text = record.getUTF8String(ordinal);
                row.writeText(name, text.toString());
                return text.numBytes();
            case Json:
                UTF8String json = record.getUTF8String(ordinal);
                row.writeJson(name, json.toString());
                return json.numBytes();
            case JsonDocument:
                UTF8String jsonDocument = record.getUTF8String(ordinal);
                row.writeJsonDocument(name, jsonDocument.toString());
                return jsonDocument.numBytes();
            case Bytes:
                ArrayData bytes = record.getArray(ordinal);
                row.writeBytes(name, bytes.toByteArray());
                return bytes.numElements();
            case Yson:
                ArrayData yson = record.getArray(ordinal);
                row.writeYson(name, yson.toByteArray());
                return yson.numElements();
            case Uuid:
                UTF8String uuid = record.getUTF8String(ordinal);
                row.writeUuid(name, UUID.fromString(uuid.toString()));
                return 16;
//            case Date:
//                record.get(ordinal, dataType)
//                row.writeDate(name, pv.getDate());
//                break;
//            case Datetime:
//                row.writeDatetime(name, pv.getDatetime());
//                break;
//            case Timestamp:
//                row.writeTimestamp(name, pv.getTimestamp());
//                break;
//            case Interval:
//                row.writeInterval(name, pv.getInterval());
//                break;
//            case Date32:
//                row.writeDate32(name, pv.getDate32());
//                break;
//            case Datetime64:
//                row.writeDatetime64(name, pv.getDatetime64());
//                break;
//            case Timestamp64:
//                row.writeTimestamp64(name, pv.getTimestamp64());
//                break;
//            case Interval64:
//                row.writeInterval64(name, pv.getInterval64());
//                break;
            default:
                throw new IllegalArgumentException("Arrow ingestion does not support type " + type);
        }
    }
}
