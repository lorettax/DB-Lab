package tinydb.optimizer;

import java.util.List;

/** Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan}
 * 返回的类指定了计划表示的最佳计划的成本和基数。
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost;
    /** The cardinality of the optimal subplan */
    public int card;
    /** The optimal subplan */
    public List<LogicalJoinNode> plan;
}
