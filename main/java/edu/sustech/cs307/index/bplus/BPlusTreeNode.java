// BPlusTreeNode.java
package edu.sustech.cs307.index.bplus;

import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class BPlusTreeNode {
    protected boolean isLeaf;
    protected List<Value> keys;

    public BPlusTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<Value> getKeys() {
        return keys;
    }

    abstract void insert(Value key, RID rid, int order, BPlusTree tree);
    abstract RID search(Value key);
}
