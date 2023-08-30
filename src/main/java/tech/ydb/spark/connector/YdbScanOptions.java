package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.connector.expressions.filter.And;

/**
 *
 * @author zinal
 */
public class YdbScanOptions implements Serializable {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YdbScanOptions.class);

    private final String catalogName;
    private final Map<String,String> connectOptions;
    private final String tablePath;
    private final String tableName;
    private final StructType schema;
    private final List<String> keyColumns;
    private final List<YdbFieldType> keyTypes;
    private final ArrayList<Object> rangeBegin;
    private final ArrayList<Object> rangeEnd;
    private int rowLimit;
    private StructType requiredSchema;

    public YdbScanOptions(YdbTable table) {
        this.catalogName = table.getConnector().getCatalogName();
        this.connectOptions = table.getConnector().getConnectOptions();
        this.tableName = table.name();
        this.tablePath = table.tablePath();
        this.schema = table.schema();
        this.keyColumns = new ArrayList<>(table.keyColumns()); // ensure serializable list
        this.keyTypes = table.keyTypes();
        this.rangeBegin = new ArrayList<>();
        this.rangeEnd = new ArrayList<>();
        this.rowLimit = -1;
    }

    public void setupPredicates(Predicate[] predicates) {
        if (predicates==null || predicates.length==0)
            return;
        List<Predicate> flat = flattenPredicates(predicates);
        detectRangeSimple(flat);
    }

    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    public StructType readSchema() {
        if (requiredSchema==null)
            return schema;
        return requiredSchema;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getConnectOptions() {
        return connectOptions;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public List<YdbFieldType> getKeyTypes() {
        return keyTypes;
    }

    public List<Object> getRangeBegin() {
        return rangeBegin;
    }

    public List<Object> getRangeEnd() {
        return rangeEnd;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    /**
     * Put all predicates connected with AND directly into the list of predicates, recursively.
     * @param filters Input filters
     * @return Flattened predicates
     */
    private List<Predicate> flattenPredicates(Predicate[] predicates) {
        final List<Predicate> retval = new ArrayList<>();
        for (Predicate p : predicates) {
            flattenPredicate(p, retval);
        }
        return retval;
    }

    /**
     * Put all filters connected with AND directly into the list of filters, recursively.
     * @param f Input filter to be processed
     * @param retval The resulting list of flattened filters
     */
    private void flattenPredicate(Predicate p, List<Predicate> retval) {
        if ("AND".equalsIgnoreCase(p.name())) {
            And fand = (And) p;
            flattenPredicate(fand.left(), retval);
            flattenPredicate(fand.right(), retval);
        } else {
            retval.add(p);
        }
    }

    /**
     * Very basic filter-to-range conversion logic.
     * Currently covers N equality conditions + 1 optional following range condition.
     * Does NOT handle complex cases like N-dimensional ranges.
     * @param predicates input list of filters
     */
    private void detectRangeSimple(List<Predicate> predicates) {
        if (predicates==null || predicates.isEmpty()) {
            return;
        }
        LOG.debug("Calculating scan ranges for predicates {}", predicates);
        rangeBegin.clear();
        rangeEnd.clear();
        for (String x : keyColumns) {
            rangeBegin.add(null);
            rangeEnd.add(null);
        }
        for (int pos = 0; pos<keyColumns.size(); ++pos) {
            final String keyColumn = keyColumns.get(pos);
            boolean hasEquality = false;
            for (Predicate p : predicates) {
                final String pname = p.name();
                if ("=".equalsIgnoreCase(pname) || "<=>".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        rangeBegin.set(pos, lyzer.value);
                        rangeEnd.set(pos, lyzer.value);
                        hasEquality = true;
                        break;
                    }
                } else if (">".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeEnd.set(pos, min(rangeEnd.get(pos), lyzer.value));
                        } else {
                            rangeBegin.set(pos, max(rangeBegin.get(pos), lyzer.value));
                        }
                        break;
                    }
                } else if (">=".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeEnd.set(pos, min(rangeEnd.get(pos), lyzer.value));
                        } else {
                            rangeBegin.set(pos, max(rangeBegin.get(pos), lyzer.value));
                        }
                        break;
                    }
                } else if ("<".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeBegin.set(pos, max(rangeBegin.get(pos), lyzer.value));
                        } else {
                            rangeEnd.set(pos, min(rangeEnd.get(pos), lyzer.value));
                        }
                        break;
                    }
                } else if ("<=".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success) {
                        if (lyzer.revert) {
                            rangeBegin.set(pos, max(rangeBegin.get(pos), lyzer.value));
                        } else {
                            rangeEnd.set(pos, min(rangeEnd.get(pos), lyzer.value));
                        }
                        break;
                    }
                } else if ("STARTS_WITH".equalsIgnoreCase(pname)) {
                    Lyzer lyzer = new Lyzer(keyColumn, p.children());
                    if (lyzer.success && !lyzer.revert) {
                        String lvalue = lyzer.value.toString();
                        if (lvalue.length() > 0) {
                            int lastCharPos = lvalue.length()-1;
                            String rvalue = new StringBuilder()
                                    .append(lvalue, 0, lastCharPos)
                                    .append((char)(1 + lvalue.charAt(lastCharPos)))
                                    .toString();
                            rangeBegin.set(pos, max(rangeBegin.get(pos), lvalue));
                            rangeEnd.set(pos, min(rangeEnd.get(pos), rvalue));
                        }
                    }
                }
            } // for (Predicate p : ...)
            if (! hasEquality)
                break;
        }

        // Drop trailing nulls
        while (! rangeBegin.isEmpty()) {
            int pos = rangeBegin.size() - 1;
            if ( rangeBegin.get(pos) == null )
                rangeBegin.remove(pos);
            else
                break;
        }
        while (! rangeEnd.isEmpty()) {
            int pos = rangeEnd.size() - 1;
            if ( rangeEnd.get(pos) == null )
                rangeEnd.remove(pos);
            else
                break;
        }
        LOG.debug("Calculated scan ranges {} -> {}", rangeBegin, rangeEnd);
    }

    private static Object max(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) > 0 ? o2 : o1;
        }
        return o2;
    }

    private static Object min(Object o1, Object o2) {
        if (o1==null || o1==o2) {
            return o2;
        }
        if (o2==null) {
            return o1;
        }
        if ((o2 instanceof Comparable) && (o1 instanceof Comparable)) {
            return ((Comparable)o2).compareTo(o1) < 0 ? o2 : o1;
        }
        return o2;
    }

    /**
     * Too small to be called "Analyzer"
     */
    static final class Lyzer {
        final boolean success;
        final boolean revert;
        final Object value;

        Lyzer(String keyColumn, Expression[] children) {
            boolean success = false;
            boolean revert = false;
            Object value = null;
            if (children.length == 2) {
                Expression left = children[0];
                Expression right = children[1];
                if (right instanceof FieldReference) {
                    Expression temp = right;
                    right = left;
                    left = temp;
                    revert = true;
                }
                if (left instanceof FieldReference
                        && left.references().length > 0
                        && right instanceof LiteralValue) {
                    NamedReference nr = left.references()[left.references().length - 1];
                    if (nr.fieldNames().length > 0) {
                        String fieldName = nr.fieldNames()[nr.fieldNames().length - 1];
                        if (keyColumn.equals(fieldName)) {
                            LiteralValue lv = (LiteralValue) right;
                            value = lv.value();
                            success = true;
                        }
                    }
                }
            }
            this.success = success;
            this.revert = revert;
            this.value = value;
        }
    }

}
