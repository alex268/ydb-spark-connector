package tech.ydb.spark.connector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Aleksandr Gorshenin <alexandr268@ydb.tech>
 */
public class TestData {
    private final boolean nullable;
    private final StructType type;

    public TestData(boolean nullable) {
        this.nullable = nullable;
        this.type = new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("col_bool", DataTypes.BooleanType, nullable, Metadata.empty()),
            new StructField("col_int8", DataTypes.ByteType, nullable, Metadata.empty()),
            new StructField("col_int16", DataTypes.ShortType, nullable, Metadata.empty()),
            new StructField("col_int32", DataTypes.IntegerType, nullable, Metadata.empty()),
            new StructField("col_int64", DataTypes.LongType, nullable, Metadata.empty()),
            new StructField("col_uint8", DataTypes.ShortType, nullable, Metadata.empty()),
            new StructField("col_uint16", DataTypes.IntegerType, nullable, Metadata.empty()),
            new StructField("col_uint32", DataTypes.LongType, nullable, Metadata.empty()),
            new StructField("col_uint64", DataTypes.createDecimalType(22, 0), nullable, Metadata.empty()),
            new StructField("col_float", DataTypes.FloatType, nullable, Metadata.empty()),
            new StructField("col_double", DataTypes.DoubleType, nullable, Metadata.empty()),
            new StructField("col_decimal22", DataTypes.createDecimalType(22, 9), nullable, Metadata.empty()),
            new StructField("col_decimal35", DataTypes.createDecimalType(35, 6), nullable, Metadata.empty()),
            new StructField("col_binary", DataTypes.BinaryType, nullable, Metadata.empty()),
            new StructField("col_text", DataTypes.StringType, nullable, Metadata.empty()),
            new StructField("col_date", DataTypes.DateType, nullable, Metadata.empty()),
            new StructField("col_date32", DataTypes.DateType, nullable, Metadata.empty()),
            new StructField("col_timestamp", DataTypes.TimestampType, nullable, Metadata.empty()),
            new StructField("col_timestamp64", DataTypes.TimestampType, nullable, Metadata.empty())
        });
    }

    public StructType getSchema() {
        return type;
    }

    public String toYqlColumns() {
        String suffix = nullable ? " ," : " NOT NULL,";
        return ""
            + "id Int32 NOT NULL,"  // pk is always NOT NULL
            + "col_bool Bool" + suffix
            + "col_int8 Int8" + suffix
            + "col_int16 Int16" + suffix
            + "col_int32 Int32" + suffix
            + "col_int64 Int64" + suffix
            + "col_uint8 Uint8" + suffix
            + "col_uint16 Uint16" + suffix
            + "col_uint32 Uint32" + suffix
            + "col_uint64 Uint64" + suffix
            + "col_float Float" + suffix
            + "col_double Double" + suffix
            + "col_decimal22 Decimal(22,9)" + suffix
            + "col_decimal35 Decimal(35,6)" + suffix
            + "col_binary Bytes" + suffix
            + "col_text Text" + suffix
            + "col_date Date" + suffix
            + "col_date32 Date32" + suffix
            + "col_timestamp Timestamp" + suffix
            + "col_timestamp64 Timestamp64" + suffix;
    }


    public ArrayList<Row> generateSet(int firstId, int lastId) {
        ArrayList<Row> rows = new ArrayList<>(lastId - firstId);

        for (int id = firstId; id < lastId; id++) {
            boolean colBool = (id % 2) != 0;
            int sign = colBool ? 1 : -1;
            byte colInt8 = (byte) (sign * (id & 0x7F));
            short colInt16 = (short) (sign * (id & 0x7FFF));
            int colInt32 = sign * id;
            long colInt64 = sign * id;

            short colUint8 = (short) (id & 0xFF);
            int colUint16 = id & 0xFFFF;
            long colUint32 = id;
            Decimal colUint64 = Decimal.apply(BigDecimal.valueOf(id));
            float colFloat = sign * 0.1234f * id;
            double colDouble = sign * 0.053d * id;
            Decimal colDecimal22 = Decimal.createUnsafe(sign * id * 100000, 22, 9);
            Decimal colDecimal35 = Decimal.createUnsafe(sign * id * 1000, 35, 6);

            byte[] colBinary = ("bytes" + id).getBytes(StandardCharsets.UTF_8);
            String colText = "text-value-" + id;

            // TODO: Check usage of default timezone
            Date colDate = new Date(Instant.ofEpochSecond(86400L * id).toEpochMilli());
            Date colDate32 = new Date(Instant.ofEpochSecond(86400L * sign * id).toEpochMilli());

            Timestamp colTimestamp = new Timestamp(86400L * id * 311);
            Timestamp colTimestamp64 = new Timestamp(86400L * sign * id * 311);

            rows.add(new GenericRowWithSchema(new Object[]{
                id,
                (nullable && id % 83 == 0) ? null : colBool,
                (nullable && id % 79 == 0) ? null : colInt8,
                (nullable && id % 73 == 0) ? null : colInt16,
                (nullable && id % 71 == 0) ? null : colInt32,
                (nullable && id % 67 == 0) ? null : colInt64,
                (nullable && id % 61 == 0) ? null : colUint8,
                (nullable && id % 59 == 0) ? null : colUint16,
                (nullable && id % 53 == 0) ? null : colUint32,
                (nullable && id % 47 == 0) ? null : colUint64,
                (nullable && id % 43 == 0) ? null : colFloat,
                (nullable && id % 37 == 0) ? null : colDouble,
                (nullable && id % 31 == 0) ? null : colDecimal22,
                (nullable && id % 29 == 0) ? null : colDecimal35,
                (nullable && id % 23 == 0) ? null : colBinary,
                (nullable && id % 19 == 0) ? null : colText,
                (nullable && id % 17 == 0) ? null : colDate,
                (nullable && id % 13 == 0) ? null : colDate32,
                (nullable && id % 11 == 0) ? null : colTimestamp,
                (nullable && id % 7 == 0)  ? null : colTimestamp64
            }, type));
        }

        return rows;
    }

}
