// InnerNode.java
package edu.sustech.cs307.index.bplus;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;

import java.util.*;

public class InnerNode extends BPlusTreeNode {
    List<BPlusTreeNode> children;

    public InnerNode() {
        super(false);
        this.children = new ArrayList<>();
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
        idx = idx >= 0 ? idx + 1 : -idx - 1;
        children.get(idx).insert(key, rid, order, tree);
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
        idx = idx >= 0 ? idx + 1 : -idx - 1;
        return children.get(idx).search(key);
    }

    void insertChild(Value key, BPlusTreeNode child) {
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

        idx = idx >= 0 ? idx + 1 : -idx - 1;
        keys.add(idx, key);
        children.add(idx + 1, child);
    }

    boolean isOverflow(int order) {
        return children.size() > order + 1;
    }

    void split(int order, BPlusTree tree) {
        int mid = keys.size() / 2;
        Value midKey = keys.get(mid);

        InnerNode sibling = new InnerNode();
        sibling.keys.addAll(keys.subList(mid + 1, keys.size()));
        sibling.children.addAll(children.subList(mid + 1, children.size()));

        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();

        tree.insertIntoParent(this, midKey, sibling);
    }
}
