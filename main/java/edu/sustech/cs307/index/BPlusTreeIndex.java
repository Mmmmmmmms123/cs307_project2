package edu.sustech.cs307.index;


import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.bplus.BPlusTree;
import edu.sustech.cs307.index.bplus.LeafNode;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;

import java.util.*;

public class BPlusTreeIndex implements Index {
    private final BPlusTree tree;

    public BPlusTreeIndex() {
        this.tree = new BPlusTree(4); // B+树阶数为4，可调
    }

    public void insert(Value key, RID rid) {
        tree.insert(key, rid);
    }

    @Override
    public RID EqualTo(Value value) {
        return tree.search(value);
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        List<Map.Entry<Value, RID>> result = new ArrayList<>();
        LeafNode node = tree.getFirstLeaf();
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                try {
                    int comparison = ValueComparer.compare(node.getKeys().get(i), value);
                    if (comparison < 0 || (isEqual && comparison == 0)) {
                        result.add(Map.entry(node.getKeys().get(i), node.getValues().get(i)));
                    }
                } catch (DBException e) {
                    throw new RuntimeException("Comparison failed in LessThan(): " + e.getMessage(), e);
                }
            }
            node = node.getNext();
        }
        return result.iterator();
    }


    @Override
    public Iterator<Map.Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        List<Map.Entry<Value, RID>> result = new ArrayList<>();
        LeafNode node = tree.getFirstLeaf();
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                try {
                    int comparison = ValueComparer.compare(node.getKeys().get(i), value);
                    if (comparison > 0 || (isEqual && comparison == 0)) {
                        result.add(Map.entry(node.getKeys().get(i), node.getValues().get(i)));
                    }
                } catch (DBException e) {
                    throw new RuntimeException("Comparison failed in MoreThan(): " + e.getMessage(), e);
                }
            }
            node = node.getNext();
        }
        return result.iterator();
    }




    @Override
    public Iterator<Map.Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        List<Map.Entry<Value, RID>> result = new ArrayList<>();
        LeafNode node = tree.getFirstLeaf();
        while (node != null) {
            for (int i = 0; i < node.getKeys().size(); i++) {
                Value k = node.getKeys().get(i);
                try {
                    int cmpLow = ValueComparer.compare(k, low);
                    int cmpHigh = ValueComparer.compare(k, high);

                    boolean leftOk = leftEqual ? cmpLow >= 0 : cmpLow > 0;
                    boolean rightOk = rightEqual ? cmpHigh <= 0 : cmpHigh < 0;

                    if (leftOk && rightOk) {
                        result.add(Map.entry(k, node.getValues().get(i)));
                    }
                } catch (DBException e) {
                    throw new RuntimeException("Comparison failed in Range(): " + e.getMessage(), e);
                }
            }
            node = node.getNext();
        }
        return result.iterator();
    }

}

