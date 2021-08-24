package tinydb.optimizer;

/** LogicalSelectListNode 代表 LogicalQueryPlan 中选择列表中的一个子句 */
public class LogicalSelectListNode {
    /** The field name being selected; the name may be (optionally) be
     * qualified with a table name or alias.
     */
    public final String fname;
   
    /** The aggregation operation over the field (if any) */
    public final String aggOp;

    public LogicalSelectListNode(String aggOp, String fname) {
        this.aggOp = aggOp;
        this.fname = fname;
    }
}
