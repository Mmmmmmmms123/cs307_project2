package edu.sustech.cs307.index.bplus;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;

import java.util.*;

public class LeafNode extends BPlusTreeNode {
    List<RID> values;
    private LeafNode next; // 将 next 设置为 private

    public LeafNode() {
        super(true);
        this.values = new ArrayList<>();
    }

    @Override
    void insert(Value key, RID rid, int order, BPlusTree tree) {
        int idx = Collections.binarySearch(
                keys,
                key,
                (v1, v2) -> {
                    try {
                        return ValueComparer.compare(v1, v2);
                    } catch (DBException e) {
                        throw new RuntimeException("Comparison failed: " + e.getMessage(), e);
                    }
                }
        );
        if (idx >= 0) {
            values.set(idx, rid);  // 覆盖
            return;
        }
        idx = -idx - 1;
        keys.add(idx, key);
        values.add(idx, rid);

        if (keys.size() > order) {
            split(order, tree);
        }
    }

    private void split(int order, BPlusTree tree) {
        int mid = keys.size() / 2;
        LeafNode newLeaf = new LeafNode();
        newLeaf.keys.addAll(keys.subList(mid, keys.size()));
        newLeaf.values.addAll(values.subList(mid, values.size()));

        keys.subList(mid, keys.size()).clear();
        values.subList(mid, values.size()).clear();

        newLeaf.next = this.next;
        this.next = newLeaf;

        tree.insertIntoParent(this, newLeaf.keys.get(0), newLeaf);
    }

    @Override
    RID search(Value key) {
        int idx = Collections.binarySearch(
                keys,
                key,
                (v1, v2) -> {
                    try {
                        return ValueComparer.compare(v1, v2);
                    } catch (DBException e) {
                        throw new RuntimeException("Comparison failed: " + e.getMessage(), e);
                    }
                }
        );
        if (idx >= 0) return values.get(idx);
        return null;
    }

    // 新增的访问方法
    public List<Value> getKeys() {
        return keys;
    }

    public List<RID> getValues() {
        return values;
    }

    public LeafNode getNext() {
        return next;
    }
}
