package tech.ydb.spark.connector.common;

/**
 * YDB data ingestion method.
 *
 * @author mzinal
 */
public enum IngestMethod {
    UPSERT,
    REPLACE,
    BULK_UPSERT;
}
