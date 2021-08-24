package tinydb.execution;
import tinydb.optimizer.LogicalJoinNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** PlanCache 是一个辅助类，可用于存储对给定连接集进行排序的最佳方式 */
public class PlanCache {
    final Map<Set<LogicalJoinNode>,List<LogicalJoinNode>> bestOrders= new HashMap<>();
    final Map<Set<LogicalJoinNode>,Double> bestCosts= new HashMap<>();
    final Map<Set<LogicalJoinNode>,Integer> bestCardinalities = new HashMap<>();
    
    /** 为特定连接集添加新的成本、基数和排序
        @param s the set of joins for which a new ordering (plan) is being added
        @param cost the estimated cost of the specified plan
        @param card the estimatied cardinality of the specified plan
        @param order the ordering of the joins in the plan
    */
    public void addPlan(Set<LogicalJoinNode> s, double cost, int card, List<LogicalJoinNode> order) {
        bestOrders.put(s,order);                        
        bestCosts.put(s,cost);
        bestCardinalities.put(s,card);
    }
    
    /** 在指定计划的缓存中找到最佳连接顺序 */
    public List<LogicalJoinNode> getOrder(Set<LogicalJoinNode> s) {
        return bestOrders.get(s);
    }
    
    /** 查找指定计划的缓存中最佳连接顺序的成本 */
    public double getCost(Set<LogicalJoinNode> s) {
        return bestCosts.get(s);
    }
    
    /**查找指定计划的缓存中最佳连接顺序的基数 */
    public int getCard(Set<LogicalJoinNode> s) {
        return bestCardinalities.get(s);
    }
}
