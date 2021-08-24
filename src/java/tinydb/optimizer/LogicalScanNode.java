package tinydb.optimizer;

import tinydb.common.Catalog;

/** LogicalScanNode 表示 LogicalQueryPlan 中 FROM 列表中的表 */
public class LogicalScanNode {

    /** The name (alias) of the table as it is used in the query */
    public final String alias;

    /** The table identifier (can be passed to {@link Catalog#getDatabaseFile})
     *   to retrieve a DbFile */
    public final int t;

    public LogicalScanNode(int table, String tableAlias) {
        this.alias = tableAlias;
        this.t = table;
    }
}

