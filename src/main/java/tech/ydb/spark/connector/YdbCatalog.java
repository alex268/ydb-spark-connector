package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.proto.scheme.SchemeOperationProtos;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.DescribePathResult;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.spark.connector.impl.YdbAlterTable;
import tech.ydb.spark.connector.impl.YdbConnector;
import tech.ydb.spark.connector.impl.YdbCreateTable;
import tech.ydb.spark.connector.impl.YdbRegistry;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 * YDB Catalog implements Spark table catalog for YDB data sources.
 *
 * @author zinal
 */
public class YdbCatalog extends YdbOptions
        implements CatalogPlugin, TableCatalog, SupportsNamespaces {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbCatalog.class);

    // X.Y[-SNAPSHOT]
    public static final String VERSION = "1.3-SNAPSHOT";
    public static final String INDEX_PREFIX = "ix/";
    public static final String ENTRY_TYPE = "ydb_entry_type";
    public static final String ENTRY_OWNER = "ydb_entry_owner";

    private String catalogName;
    private YdbConnector connector;
    private boolean listIndexes;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.connector = YdbRegistry.getOrCreate(name, options);
        this.listIndexes = options.getBoolean(LIST_INDEXES, false);
    }

    @Override
    public String name() {
        return catalogName;
    }

    private YdbConnector getConnector() {
        if (connector == null) {
            throw new IllegalStateException("Catalog " + catalogName + " not initialized");
        }
        return connector;
    }

    private SchemeClient getSchemeClient() {
        return getConnector().getSchemeClient();
    }

    private SessionRetryContext getRetryCtx() {
        return getConnector().getRetryCtx();
    }

    public static <T> T checkStatus(Result<T> res, String[] namespace)
            throws NoSuchNamespaceException {
        if (!res.isSuccess()) {
            final Status status = res.getStatus();
            if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                for (Issue i : status.getIssues()) {
                    if (i != null && i.getMessage().endsWith("Path not found")) {
                        throw new NoSuchNamespaceException(namespace);
                    }
                }
            }
            status.expectSuccess("ydb metadata query failed on " + Arrays.toString(namespace));
        }
        return res.getValue();
    }

    public static <T> T checkStatus(Result<T> res, Identifier id)
            throws NoSuchTableException {
        if (!res.isSuccess()) {
            Status status = res.getStatus();
            if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                throw new NoSuchTableException(id);
            }
            status.expectSuccess("ydb metadata query failed on " + id);
        }
        return res.getValue();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        try {
            Result<ListDirectoryResult> res = getSchemeClient()
                    .listDirectory(mergePath(namespace)).join();
            ListDirectoryResult ldr = checkStatus(res, namespace);
            List<Identifier> retval = new ArrayList<>();
            for (SchemeOperationProtos.Entry e : ldr.getChildren()) {
                if (SchemeOperationProtos.Entry.Type.TABLE.equals(e.getType())) {
                    retval.add(Identifier.of(namespace, e.getName()));
                    if (listIndexes) {
                        listIndexes(namespace, retval, e);
                    }
                } else if (SchemeOperationProtos.Entry.Type.COLUMN_TABLE.equals(e.getType())) {
                    retval.add(Identifier.of(namespace, e.getName()));
                }
            }
            return retval.toArray(new Identifier[0]);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void listIndexes(String[] namespace, List<Identifier> retval,
            SchemeOperationProtos.Entry tableEntry) {
        String tablePath = mergePath(namespace, tableEntry.getName());
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            return session.describeTable(tablePath, new DescribeTableSettings());
        }).join();
        if (!res.isSuccess()) {
            // Skipping problematic entries.
            LOG.warn("Skipping index listing for table {} due to failed describe, status {}",
                    tablePath, res.getStatus());
            return;
        }
        TableDescription td = res.getValue();
        for (TableIndex ix : td.getIndexes()) {
            String ixname = INDEX_PREFIX + tableEntry.getName() + "/" + ix.getName();
            retval.add(Identifier.of(namespace, ixname));
        }
    }

    @Override
    public YdbTable loadTable(Identifier ident) throws NoSuchTableException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            // Special support for index "tables".
            String pseudoName = ident.name();
            String[] tabParts = pseudoName.split("[/]");
            if (tabParts.length != 3) {
                // Illegal name format - so "no such table".
                throw new NoSuchTableException(ident);
            }
            String tabName = tabParts[1];
            String ixName = tabParts[2];
            String tablePath = mergePath(ident.namespace(), tabName);
            return checkStatus(YdbTable.lookup(connector, connector.getDefaultTypes(),
                    tablePath, mergeLocal(ident), ixName), ident);
        }
        // Processing for regular tables.
        String tablePath = mergePath(ident);
        return checkStatus(YdbTable.lookup(connector, connector.getDefaultTypes(),
                tablePath, mergeLocal(ident), null), ident);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
            Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Direct index table creation is not possible,"
                    + "identifier " + ident);
        }
        String tablePath = mergePath(ident);
        // Actual table creation logic is moved to a separate class.
        final YdbCreateTable action = new YdbCreateTable(tablePath,
                YdbCreateTable.convert(getConnector().getDefaultTypes(), schema),
                properties);
        getRetryCtx().supplyStatus(session -> action.createTable(session)).join()
                .expectSuccess("Failed to create table " + ident);
        // Load the description for the table created.
        try {
            return checkStatus(YdbTable.lookup(connector, connector.getDefaultTypes(),
                    tablePath, mergeLocal(ident), null), ident);
        } catch (NoSuchTableException nste) {
            throw new RuntimeException("Lost table after creation on id " + ident);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Index table alteration is not possible, "
                    + "identifier " + ident);
        }
        final String tablePath = mergePath(ident);
        // Load the current table description and set up the operation.
        final YdbAlterTable operation = new YdbAlterTable(connector, tablePath);
        // Prepare for processing and ensure that all changes are of supported types.
        for (TableChange change : changes) {
            if (change instanceof TableChange.AddColumn) {
                operation.prepare((TableChange.AddColumn) change);
            } else if (change instanceof TableChange.DeleteColumn) {
                operation.prepare((TableChange.DeleteColumn) change);
            } else if (change instanceof TableChange.SetProperty) {
                operation.prepare((TableChange.SetProperty) change);
            } else if (change instanceof TableChange.RemoveProperty) {
                operation.prepare((TableChange.RemoveProperty) change);
            } else {
                throw new UnsupportedOperationException("YDB table alter operation not supported: " + change);
            }
        }
        // Implement the desired changes.
        getRetryCtx().supplyStatus(session -> operation.run(session)).join().expectSuccess();
        // Load the description for the modified table.
        return checkStatus(YdbTable.lookup(connector, connector.getDefaultTypes(),
                tablePath, mergeLocal(ident), null), ident);
    }

    @Override
    public boolean dropTable(Identifier ident) {
        if (ident.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot drop index table " + ident);
        }
        final String tablePath = mergePath(ident);
        LOG.debug("Dropping table {}", tablePath);
        Result<TableDescription> res = getRetryCtx().supplyResult(session -> {
            final DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(false);
            return session.describeTable(tablePath, dts);
        }).join();
        try {
            checkStatus(res, ident);
        } catch (NoSuchTableException nste) {
            return false;
        }
        Status status = connector.getRetryCtx().supplyStatus(
                session -> session.dropTable(tablePath)).join();
        if (!status.isSuccess()) {
            status.expectSuccess("Failed to drop table " + ident);
        }
        return true;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        if (oldIdent.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot rename index table " + oldIdent);
        }
        if (newIdent.name().startsWith(INDEX_PREFIX)) {
            throw new UnsupportedOperationException("Cannot rename table to index " + newIdent);
        }
        final String oldPath = mergePath(oldIdent);
        final String newPath = mergePath(newIdent);
        Status status = getRetryCtx().supplyStatus(
                session -> session.renameTable(oldPath, newPath, false)).join();
        if (!status.isSuccess()) {
            status.expectSuccess("Failed to rename table [" + oldIdent + "] to [" + newIdent + "]");
        }
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(null);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null) {
            namespace = new String[0];
        }
        try {
            Result<ListDirectoryResult> res = getSchemeClient()
                    .listDirectory(mergePath(namespace)).get();
            ListDirectoryResult ldr = checkStatus(res, namespace);
            List<String[]> retval = new ArrayList<>();
            for (SchemeOperationProtos.Entry e : ldr.getChildren()) {
                if (SchemeOperationProtos.Entry.Type.DIRECTORY.equals(e.getType())) {
                    final String[] x = new String[namespace.length + 1];
                    System.arraycopy(namespace, 0, x, 0, namespace.length);
                    x[namespace.length] = e.getName();
                    retval.add(x);
                }
            }
            return retval.toArray(new String[0][0]);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        if (namespace == null || namespace.length == 0) {
            return Collections.emptyMap();
        }
        final Map<String, String> m = new HashMap<>();
        Result<DescribePathResult> res = getSchemeClient()
                .describePath(mergePath(namespace)).join();
        DescribePathResult dpr = checkStatus(res, namespace);
        m.put(ENTRY_TYPE, dpr.getSelf().getType().name());
        m.put(ENTRY_OWNER, dpr.getSelf().getOwner());
        return m;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        Status status = getSchemeClient().makeDirectory(mergePath(namespace)).join();
        if (status.isSuccess()
                && status.getIssues() != null
                && status.getIssues().length > 0) {
            for (Issue i : status.getIssues()) {
                String msg = i.getMessage();
                if (msg != null && msg.contains(" path exist, request accepts it")) {
                    throw new NamespaceAlreadyExistsException(namespace);
                }
            }
        }
        status.expectSuccess();
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean recursive)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        if (!recursive) {
            Status status = getSchemeClient().removeDirectory(mergePath(namespace)).join();
            return status.isSuccess();
        } else {
            // TODO: recursive removal
            throw new UnsupportedOperationException("Recursive namespace removal is not implemented");
        }
    }

    private String getDatabase() {
        return connector.getDatabase();
    }

    private static String safeName(String v) {
        if (v == null) {
            return "";
        }
        if (v.contains("/")) {
            v = v.replace("/", "_");
        }
        if (v.contains("\\")) {
            v = v.replace("\\", "_");
        }
        return v;
    }

    private void mergeLocal(String[] items, StringBuilder sb) {
        if (items != null) {
            for (String i : items) {
                if (sb.length() > 0) {
                    sb.append("/");
                }
                sb.append(safeName(i));
            }
        }
    }

    private void mergeLocal(Identifier id, StringBuilder sb) {
        mergeLocal(id.namespace(), sb);
        if (sb.length() > 0) {
            sb.append("/");
        }
        sb.append(safeName(id.name()));
    }

    private String mergeLocal(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        mergeLocal(id, sb);
        return sb.toString();
    }

    private String mergePath(String[] items) {
        if (items == null || items.length == 0) {
            return getDatabase();
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(items, sb);
        return sb.toString();
    }

    private String mergePath(String[] items, String extra) {
        if (extra == null) {
            return mergePath(items);
        }
        if (items == null) {
            return mergePath(new String[]{extra});
        }
        String[] work = new String[1 + items.length];
        System.arraycopy(items, 0, work, 0, items.length);
        work[items.length] = extra;
        return mergePath(work);
    }

    private String mergePath(Identifier id) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getDatabase());
        mergeLocal(id, sb);
        return sb.toString();
    }

}
