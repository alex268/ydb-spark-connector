package tech.ydb.spark.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tech.ydb.test.junit4.YdbHelperRule;

/**
 * Tests predicate pushdown for YDB columns of various data types.
 * Uses Int32 primary key and columns of types: Bool (row) / Int8 (column), Int8, Int16, Int32, Int64,
 * Uint8, Uint16, Uint32, Uint64, Float, Double, Decimal(22,9), Decimal(35,6), Bytes, Text,
 * Date, Date32, Timestamp, Timestamp64.
 *
 * Three table flavors: row-organized single partition (Bool), row-organized 10 partitions (Bool),
 * and column-organized (Int8 for bool-like, Bool not supported in column store). Each has 1k rows.
 * Validates &lt;, &gt;, &lt;=, &gt;= predicates.
 */

public class DataTypesPredicatesTest {

    private static final TestData DATA = new TestData(false);
    private static final int ROW_COUNT = 2000;
    private static final String DS_SINGLE_TABLE = "datatypes_ds_single_table";
    private static final String DS_PARTITIONED_TABLE = "datatypes_ds_partitioned_table";
    private static final String CS_TABLE = "datatypes_cs_table";

    @ClassRule
    public static final YdbHelperRule YDB = new YdbHelperRule();

    private static Map<String, String> ydbCreds;
    private static SparkSession spark;
    private static Dataset<Row> sourceData;

    @BeforeClass
    public static void prepare() {
        ydbCreds = new HashMap<>();
        ydbCreds.put("url", new StringBuilder()
                .append(YDB.useTls() ? "grpcs://" : "grpc://")
                .append(YDB.endpoint())
                .append(YDB.database())
                .toString());
        ydbCreds.put("table.autocreate", "false");

        if (YDB.authToken() != null) {
            ydbCreds.put("auth.token", YDB.authToken());
        }

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("ydb-spark-datatypes-predicates-test")
                .set("spark.ui.enabled", "false");

        spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        sourceData = spark.createDataFrame(DATA.generateSet(0, ROW_COUNT), DATA.getSchema());
        initTables();
    }

    @AfterClass
    public static void close() throws IOException {
        if (spark != null) {
            dropTables();
            spark.close();
        }
        YdbRegistry.closeAll();
    }

    private static void initTables() {
        String yql = DATA.toYqlColumns();
        // Row table, single partition (no explicit partition keys)
        readYdb().option("query", "CREATE TABLE `" + DS_SINGLE_TABLE + "`"
                + "(" + yql + "PRIMARY KEY(id)) "
                + "WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)"
        ).load().count();

        // Row table, 10 partitions
        readYdb().option("query", "CREATE TABLE `" + DS_PARTITIONED_TABLE + "`"
                + "(" + yql + "PRIMARY KEY(id))"
                + "WITH ("
                + "  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10, "
                + "  PARTITION_AT_KEYS = (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000)"
                + ")"
        ).load().count();

        // Column table
        readYdb().option("query", "CREATE TABLE `" + CS_TABLE + "` ("
                + yql
                + "PRIMARY KEY(id)) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=8)"
        ).load().count();

        sourceData.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save(DS_SINGLE_TABLE);
        sourceData.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save(DS_PARTITIONED_TABLE);
        sourceData.write().format("ydb").options(ydbCreds).mode(SaveMode.Append).save(CS_TABLE);

        Assert.assertEquals(ROW_COUNT, readTable(DS_SINGLE_TABLE).count());
        Assert.assertEquals(ROW_COUNT, readTable(DS_PARTITIONED_TABLE).count());
        Assert.assertEquals(ROW_COUNT, readTable(CS_TABLE).count());
    }

    private static void dropTables() {
        readYdb().option("query", ""
                + "DROP TABLE IF EXISTS `" + DS_SINGLE_TABLE + "`; "
                + "DROP TABLE IF EXISTS `" + DS_PARTITIONED_TABLE + "`; "
                + "DROP TABLE IF EXISTS `" + CS_TABLE + "`;"
        ).load().count();
    }

    private static DataFrameReader readYdb() {
        return spark.read().format("ydb").options(ydbCreds);
    }

    private static Dataset<Row> readTable(String tableName) {
        return readYdb().option("pushDownPredicate", "true").load(tableName);
    }

    private void assertPredicateCount(String filter, long expectedCount) {
        Assert.assertEquals("Source data validate", expectedCount, sourceData.filter(filter).count());
        Assert.assertEquals("Single partition table", expectedCount, readTable(DS_SINGLE_TABLE).filter(filter).count());
        Assert.assertEquals("Partitioned table", expectedCount, readTable(DS_PARTITIONED_TABLE).filter(filter).count());
        Assert.assertEquals("Column shard table", expectedCount, readTable(CS_TABLE).filter(filter).count());
    }

    @Test
    public void predicatesTest() {
        List<CompletableFuture<AssertionError>> tests = new ArrayList<>();

        tests.add(runTest(this::booleanPredicateTest));
        tests.add(runTest(this::int8PredicateTest));
        tests.add(runTest(this::int16PredicateTest));
        tests.add(runTest(this::int32PredicateTest));
        tests.add(runTest(this::int64PredicateTest));

        tests.add(runTest(this::uint8PredicateTest));
        tests.add(runTest(this::uint16PredicateTest));
        tests.add(runTest(this::uint32PredicateTest));
        tests.add(runTest(this::uint64PredicateTest));

        tests.add(runTest(this::floatPredicateTest));
        tests.add(runTest(this::doublePredicateTest));

        tests.add(runTest(this::datePredicateTest));
        tests.add(runTest(this::date32PredicateTest));

        tests.add(runTest(this::timestampPredicateTest));
        tests.add(runTest(this::timestamp64PredicateTest));

        for (CompletableFuture<AssertionError> future: tests) {
            AssertionError ex = future.join();
            if (ex != null) {
                throw ex;
            }
        }
    }

    private CompletableFuture<AssertionError> runTest(Runnable runnable) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                runnable.run();
                return (AssertionError) null;
            } catch (AssertionError ex) {
                return ex;
            }
        });
    }

    public void booleanPredicateTest() {
        assertPredicateCount("col_bool = true", 1000);
        assertPredicateCount("col_bool = false", 1000);
        assertPredicateCount("col_bool < true", 1000);
        assertPredicateCount("col_bool >= false", 2000);
    }

    public void int8PredicateTest() {
        assertPredicateCount("col_int8 <= 0", 1000);
        assertPredicateCount("col_int8 > -100", 1790);
        assertPredicateCount("col_int8 <= -100", 210);
        assertPredicateCount("col_int8 >= 128", 0);
    }

    public void int16PredicateTest() {
        assertPredicateCount("col_int16 <= 0", 1000);
        assertPredicateCount("col_int16 >= -250", 1126);
        assertPredicateCount("col_int16 < -250", 874);
        assertPredicateCount("col_int16 = 201", 1);
    }

    public void int32PredicateTest() {
        assertPredicateCount("col_int32 > 0", 1000);
        assertPredicateCount("col_int32 >= -1", 1001);
        assertPredicateCount("col_int32 < -1", 999);
        assertPredicateCount("col_int32 = -700", 1);
    }

    public void int64PredicateTest() {
        assertPredicateCount("col_int64 > 0", 1000);
        assertPredicateCount("col_int64 >= -1", 1001);
        assertPredicateCount("col_int64 < -1", 999);
        assertPredicateCount("col_int64 = -700", 1);
    }

    public void uint8PredicateTest() {
        assertPredicateCount("col_uint8 < 0", 0);
        assertPredicateCount("col_uint8 > 128", 968);
        assertPredicateCount("col_uint8 <= 200", 1608);
        assertPredicateCount("col_uint8 = 127", 8);
    }

    public void uint16PredicateTest() {
        assertPredicateCount("col_uint16 < 0", 0);
        assertPredicateCount("col_uint16 > 128", 1871);
        assertPredicateCount("col_uint16 <= 200", 201);
        assertPredicateCount("col_uint16 = 127", 1);
    }

    public void uint32PredicateTest() {
        assertPredicateCount("col_uint32 < 0", 0);
        assertPredicateCount("col_uint32 > 128", 1871);
        assertPredicateCount("col_uint32 <= 200", 201);
        assertPredicateCount("col_uint32 = 127", 1);
    }

    public void uint64PredicateTest() {
        assertPredicateCount("col_uint64 < 0", 0);
        assertPredicateCount("col_uint64 > 128", 1871);
        assertPredicateCount("col_uint64 <= 200", 201);
        assertPredicateCount("col_uint64 = 127", 1);
    }

    public void floatPredicateTest() {
        assertPredicateCount("col_float > 0f", 1000);
        assertPredicateCount("col_float < -100f", 594);
        assertPredicateCount("col_float >= -100f", 1406);
        assertPredicateCount("col_float <= 100f", 1405);
    }

    public void doublePredicateTest() {
        assertPredicateCount("col_double > 0d", 1000);
        assertPredicateCount("col_double < -10d", 905);
        assertPredicateCount("col_double >= -10d", 1095);
        assertPredicateCount("col_double <= 10d", 1094);
    }

    public void datePredicateTest() {
        assertPredicateCount("col_date >= date'1960-01-01'", 2000);
        assertPredicateCount("col_date < date'1970-01-01'", 0);
        assertPredicateCount("col_date <= date'1972-01-01'", 731);
        assertPredicateCount("col_date = date'1972-02-03'", 1);
    }

    public void date32PredicateTest() {
        assertPredicateCount("col_date32 >= date'1960-01-01'", 2000);
        assertPredicateCount("col_date32 < date'1970-01-01'", 999);
        assertPredicateCount("col_date32 <= date'1972-01-01'", 1365);
        assertPredicateCount("col_date32 = date'1972-02-03'", 1);
    }

    public void timestampPredicateTest() {
        assertPredicateCount("col_timestamp >= timestamp'1960-01-01 00:00:00'", 2000);
        assertPredicateCount("col_timestamp < timestamp'1970-01-01 00:00:00'", 0);
        assertPredicateCount("col_timestamp <= timestamp'1971-01-01 00:00:00'", 1174);
        assertPredicateCount("col_timestamp = timestamp'1970-01-03 19:10:33.6Z'", 1);
    }

    public void timestamp64PredicateTest() {
        assertPredicateCount("col_timestamp64 >= timestamp'1960-01-01 00:00:00'", 2000);
        assertPredicateCount("col_timestamp64 < timestamp'1970-01-01 00:00:00'", 999);
        assertPredicateCount("col_timestamp64 <= timestamp'1971-01-01 00:00:00'", 1587);
        assertPredicateCount("col_timestamp64 = timestamp'1970-01-03 19:10:33.6Z'", 1);
    }
}
