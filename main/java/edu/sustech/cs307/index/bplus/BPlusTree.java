// BPlusTree.java
package edu.sustech.cs307.index.bplus;

import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

public class BPlusTree {
    private int order;
    private BPlusTreeNode root;

    public BPlusTree(int order) {
        this.order = order;
        this.root = new LeafNode();
    }

    public void insert(Value key, RID rid) {
        root.insert(key, rid, order, this);
    }

    public RID search(Value key) {
        return root.search(key);
    }

    void insertIntoParent(BPlusTreeNode left, Value key, BPlusTreeNode right) {
        if (root == left) {
            InnerNode newRoot = new InnerNode();
            newRoot.keys.add(key);
            newRoot.children.add(left);
            newRoot.children.add(right);
            root = newRoot;
            return;
        }

        // 简化：不追踪父节点，建议改用 stack 或父指针实现完整逻辑
        throw new UnsupportedOperationException("Parent pointer tracking not implemented");
    }

    public LeafNode getFirstLeaf() {
        BPlusTreeNode curr = root;
        while (!curr.isLeaf()) {
            curr = ((InnerNode) curr).children.get(0);
        }
        return (LeafNode) curr;
    }
}
