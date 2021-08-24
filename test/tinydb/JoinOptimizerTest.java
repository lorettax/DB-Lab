package tinydb;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tinydb.common.Database;
import tinydb.common.Utility;
import tinydb.execution.Predicate;
import tinydb.optimizer.JoinOptimizer;
import tinydb.optimizer.LogicalJoinNode;
import tinydb.optimizer.TableStats;
import tinydb.storage.BufferPool;
import tinydb.storage.HeapFile;
import tinydb.storage.HeapFileEncoder;
import tinydb.systemtest.SimpleDbTestBase;
import tinydb.systemtest.SystemTestUtil;
import tinydb.transaction.TransactionId;

public class JoinOptimizerTest extends SimpleDbTestBase {


    public static HeapFile createDuplicateHeapFile(
            List<List<Integer>> tuples, int columns, String colPrefix)
            throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.getPageSize(), columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
    }

    List<List<Integer>> tuples1;
    HeapFile f1;
    String tableName1;
    int tableId1;
    TableStats stats1;

    List<List<Integer>> tuples2;
    HeapFile f2;
    String tableName2;
    int tableId2;
    TableStats stats2;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create some sample tables to work with
        this.tuples1 = new ArrayList<>();
        this.f1 = SystemTestUtil.createRandomHeapFile(10, 1000, 20, null,
                tuples1, "c");

        this.tableName1 = "TA";
        Database.getCatalog().addTable(f1, tableName1);
        this.tableId1 = Database.getCatalog().getTableId(tableName1);
        System.out.println("tableId1: " + tableId1);

        stats1 = new TableStats(tableId1, 19);
        TableStats.setTableStats(tableName1, stats1);

        this.tuples2 = new ArrayList<>();
        this.f2 = SystemTestUtil.createRandomHeapFile(10, 10000, 20, null,
                tuples2, "c");

        this.tableName2 = "TB";
        Database.getCatalog().addTable(f2, tableName2);
        this.tableId2 = Database.getCatalog().getTableId(tableName2);
        System.out.println("tableId2: " + tableId2);

        stats2 = new TableStats(tableId2, 19);

        TableStats.setTableStats(tableName2, stats2);
    }

    private double[] getRandomJoinCosts(JoinOptimizer jo, LogicalJoinNode js,
                                        int[] card1s, int[] card2s, double[] cost1s, double[] cost2s) {
        double[] ret = new double[card1s.length];
        for (int i = 0; i < card1s.length; ++i) {
            ret[i] = jo.estimateJoinCost(js, card1s[i], card2s[i], cost1s[i],
                    cost2s[i]);
            // assert that he join cost is no less than the total cost of
            // scanning two tables
            Assert.assertTrue(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }

    @Test
    public void estimateJoinCostTest() throws ParsingException, IOException {
        // It's hard to narrow these down much at all, because students
        // may have implemented custom join algorithms.
        // So, just make sure the orders of the return values make sense.

        TransactionId tid = new TransactionId();
        JoinOptimizer jo;
        Parser p = new Parser();
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName2
                + " t2 WHERE t1.c1 = t2.c2;"), new ArrayList<>());
        // 1 join 2
        LogicalJoinNode equalsJoinNode = new LogicalJoinNode(tableName1,
                tableName2, Integer.toString(1), Integer.toString(2),
                Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 2 join 1
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName2
                + " t2 WHERE t1.c1 = t2.c2;"), new ArrayList<>());
        equalsJoinNode = new LogicalJoinNode(tableName2, tableName1,
                Integer.toString(2), Integer.toString(1), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 1 join 1
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName1 + " t1, " + tableName1
                + " t2 WHERE t1.c3 = t2.c4;"), new ArrayList<>());
        equalsJoinNode = new LogicalJoinNode(tableName1, tableName1,
                Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
        // 2 join 2
        jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM "
                + tableName2 + " t1, " + tableName2
                + " t2 WHERE t1.c8 = t2.c7;"), new ArrayList<>());
        equalsJoinNode = new LogicalJoinNode(tableName2, tableName2,
                Integer.toString(8), Integer.toString(7), Predicate.Op.EQUALS);
        checkJoinEstimateCosts(jo, equalsJoinNode);
    }

    private void checkJoinEstimateCosts(JoinOptimizer jo,
            LogicalJoinNode equalsJoinNode) {
        int[] card1s = new int[20];
        int[] card2s = new int[card1s.length];
        double[] cost1s = new double[card1s.length];
        double[] cost2s = new double[card1s.length];
        Object[] ret;
        // card1s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 3 * i + 1;
            card2s[i] = 5;
            cost1s[i] = cost2s[i] = 5.0;
        }
        double[] stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s,
                cost1s, cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // card2s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 4;
            card2s[i] = 3 * i + 1;
            cost1s[i] = cost2s[i] = 5.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // cost1s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = card2s[i] = 7;
            cost1s[i] = 5.0 * (i + 1);
            cost2s[i] = 3.0;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // cost2s linear others constant
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = card2s[i] = 9;
            cost1s[i] = 5.0;
            cost2s[i] = 3.0 * (i + 1);
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkLinear(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
        // everything linear
        for (int i = 0; i < card1s.length; ++i) {
            card1s[i] = 2 * (i + 1);
            card2s[i] = 9 * i + 1;
            cost1s[i] = 5.0 * i + 2;
            cost2s[i] = 3.0 * i + 1;
        }
        stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s,
                cost2s);
        ret = SystemTestUtil.checkQuadratic(stats);
        Assert.assertEquals(Boolean.TRUE, ret[0]);
    }

    @Test
    public void estimateJoinCardinality() throws ParsingException, IOException {
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
        JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid,
                "SELECT * FROM " + tableName2 + " t1, " + tableName2
                        + " t2 WHERE t1.c8 = t2.c7;"),
                new ArrayList<>());

        double cardinality;

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + 3, "c" + 4,
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.8),
                stats2.estimateTableCardinality(0.2), true, false, TableStats
                        .getStatsMap());

        Assert.assertTrue(cardinality == 800 || cardinality == 2000);

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + 3, "c" + 4,
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.8),
                stats2.estimateTableCardinality(0.2), false, true, TableStats
                        .getStatsMap());

        Assert.assertTrue(cardinality == 800 || cardinality == 2000);
    }

    @Test
    public void orderJoinsTest() throws ParsingException, IOException {

        final int IO_COST = 101;

        // Create a whole bunch of variables that we're going to use
        TransactionId tid = new TransactionId();
        JoinOptimizer j;
        List<LogicalJoinNode> result;
        List<LogicalJoinNode> nodes = new ArrayList<>();
        Map<String, TableStats> stats = new HashMap<>();
        Map<String, Double> filterSelectivities = new HashMap<>();

        // Create all of the tables, and add them to the catalog
        List<List<Integer>> empTuples = new ArrayList<>();
        HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null,
                empTuples, "c");
        Database.getCatalog().addTable(emp, "emp");

        List<List<Integer>> deptTuples = new ArrayList<>();
        HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null,
                deptTuples, "c");
        Database.getCatalog().addTable(dept, "dept");

        List<List<Integer>> hobbyTuples = new ArrayList<>();
        HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null,
                hobbyTuples, "c");
        Database.getCatalog().addTable(hobby, "hobby");

        List<List<Integer>> hobbiesTuples = new ArrayList<>();
        HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null,
                hobbiesTuples, "c");
        Database.getCatalog().addTable(hobbies, "hobbies");

        // Get TableStats objects for each of the tables that we just generated.
        stats.put("emp", new TableStats(
                Database.getCatalog().getTableId("emp"), IO_COST));
        stats.put("dept",
                new TableStats(Database.getCatalog().getTableId("dept"),
                        IO_COST));
        stats.put("hobby",
                new TableStats(Database.getCatalog().getTableId("hobby"),
                        IO_COST));
        stats.put("hobbies",
                new TableStats(Database.getCatalog().getTableId("hobbies"),
                        IO_COST));

        filterSelectivities.put("emp", 0.1);
        filterSelectivities.put("dept", 1.0);
        filterSelectivities.put("hobby", 1.0);
        filterSelectivities.put("hobbies", 1.0);


        nodes.add(new LogicalJoinNode("hobbies", "hobby", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "dept", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "hobbies", "c2", "c0",
                Predicate.Op.EQUALS));
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;"),
                nodes);

        result = j.orderJoins(stats, filterSelectivities, false);

        // There are only three join nodes;
        Assert.assertEquals(result.size(), nodes.size());

        Assert.assertNotSame("hobbies", result.get(0).t1Alias);

        Assert.assertFalse(result.get(2).t2Alias.equals("hobbies")
                && (result.get(0).t1Alias.equals("hobbies") || result.get(0).t2Alias.equals("hobbies")));
    }

    @Test(timeout = 60000)
    public void bigOrderJoinsTest() throws IOException,
            ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        Map<String, TableStats> stats = new HashMap<>();
        List<LogicalJoinNode> result;
        List<LogicalJoinNode> nodes = new ArrayList<>();
        Map<String, Double> filterSelectivities = new HashMap<>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        List<List<Integer>> smallHeapFileTuples = new ArrayList<>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100,
                Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");

        List<List<Integer>> bigHeapFileTuples = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            bigHeapFileTuples.add(smallHeapFileTuples.get(i % 100));
        }
        HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2,
                "c");
        Database.getCatalog().addTable(bigHeapFile, "bigTable");

        // Add the tables to the database
        Database.getCatalog().addTable(bigHeapFile, "bigTable");
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");
        Database.getCatalog().addTable(smallHeapFileJ, "j");
        Database.getCatalog().addTable(smallHeapFileK, "k");
        Database.getCatalog().addTable(smallHeapFileL, "l");
        Database.getCatalog().addTable(smallHeapFileM, "m");
        Database.getCatalog().addTable(smallHeapFileN, "n");

        // Come up with join statistics for the tables
        stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("j", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("k", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("l", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("m", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("n", new TableStats(smallHeapFileG.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("bigTable", 1.0);
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);
        filterSelectivities.put("j", 1.0);
        filterSelectivities.put("k", 1.0);
        filterSelectivities.put("l", 1.0);
        filterSelectivities.put("m", 1.0);
        filterSelectivities.put("n", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("i", "j", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("j", "k", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("k", "l", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("l", "m", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("m", "n", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("n", "bigTable", "c0", "c0",
                Predicate.Op.EQUALS));

        // Make sure we don't give the nodes to the optimizer in a nice order
        Collections.shuffle(nodes);
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;"),
                nodes);

        // Set the last boolean here to 'true' in order to have orderJoins()
        // print out its logic
        result = j.orderJoins(stats, filterSelectivities, false);

        Assert.assertEquals(result.size(), nodes.size());

        Assert.assertEquals(result.get(result.size() - 1).t2Alias, "bigTable");
    }

    @Test
    public void nonequalityOrderJoinsTest() throws IOException,
            ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        Map<String, TableStats> stats = new HashMap<>();
        List<LogicalJoinNode> result;
        List<LogicalJoinNode> nodes = new ArrayList<>();
        Map<String, Double> filterSelectivities = new HashMap<>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        List<List<Integer>> smallHeapFileTuples = new ArrayList<>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100,
                Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");

        // Add the tables to the database
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");

        // Come up with join statistics for the tables
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileH.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileI.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1",
                Predicate.Op.LESS_THAN));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));

        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT COUNT(a.c0) FROM a, b, c, d,e,f,g,h,i WHERE a.c1 < b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0;"),
                nodes);

        result = j.orderJoins(stats, filterSelectivities, false);

        Assert.assertEquals(result.size(), nodes.size());

        Assert.assertTrue(result.get(result.size() - 1).t2Alias.equals("a")
                || result.get(result.size() - 1).t1Alias.equals("a"));
    }
}
