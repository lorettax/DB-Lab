package tinydb.execution;

import tinydb.transaction.TransactionAbortedException;
import tinydb.common.Type;
import tinydb.common.DbException;
import tinydb.storage.Tuple;
import tinydb.storage.TupleDesc;

import java.util.*;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private final List<Integer> outFieldIds;

    /**
     * 构造函数接受一个子运算符来读取要应用投影的元组和输出元组中的字段列表
     *
     * @param fieldList 要投影出的字段 child 的 tupleDesc 的 ID
     * @param typesList 最终投影中的字段类型
     * @param child     The child operator
     */
    public Project(List<Integer> fieldList, List<Type> typesList,
                   OpIterator child) {
        this(fieldList, typesList.toArray(new Type[]{}), child);
    }

    public Project(List<Integer> fieldList, Type[] types,
                   OpIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childtd = child.getTupleDesc();

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childtd.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(types, fieldAr);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Operator.fetchNext 实现。迭代来自子运算符的元组，从元组中突出字段
     *
     * @return The next tuple, or null if there are no more tuples
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!child.hasNext()) {
            return null;
        }
        Tuple t = child.next();
        Tuple newTuple = new Tuple(td);
        newTuple.setRecordId(t.getRecordId());
        for (int i = 0; i < td.numFields(); i++) {
            newTuple.setField(i, t.getField(outFieldIds.get(i)));
        }
        return newTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
