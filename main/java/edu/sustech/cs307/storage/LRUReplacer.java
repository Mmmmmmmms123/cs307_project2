package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    // pinned 页面不会被淘汰
    private final Set<Integer> pinnedFrames = new HashSet<>();

    // LRU 列表保存的是可淘汰的 frameId，最前面是最久没使用的
    private final Set<Integer> LRUHash = new HashSet<>(); // 保证查询效率
    private final LinkedList<Integer> LRUList = new LinkedList<>(); // 保证顺序

    // 记录所有尚未被淘汰的页面（无论 pinned 还是在 LRU 中）
    private final Set<Integer> activeFrames = new HashSet<>();

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    // 淘汰最近最久未使用、未被 pinned 的页面
    public int Victim() {
        if (LRUList.isEmpty()) return -1;
        int victim = LRUList.removeFirst();
        LRUHash.remove(victim);
        activeFrames.remove(victim);
        return victim;
    }

    // 页正在使用，不可淘汰，从 LRU 中移除
    public void Pin(int frameId) {
        // 如果已经在 pinned 状态，忽略
        if (pinnedFrames.contains(frameId)) return;

        // 如果该页在 LRU 中，说明之前 Unpin 过，现在重新 Pin，应从 LRU 中移除
        if (LRUHash.contains(frameId)) {
            LRUHash.remove(frameId);
            LRUList.remove((Integer) frameId);
            pinnedFrames.add(frameId);
            return;
        }

        // 如果新页总数超限
        if (size() >= maxSize) {
            throw new RuntimeException("REPLACER IS FULL");
        }

        pinnedFrames.add(frameId);
        activeFrames.add(frameId);
    }

    // 页不再使用，可参与淘汰，加入 LRU 尾部
    public void Unpin(int frameId) {
        // 不存在的 frameId 不允许 Unpin（可能被淘汰了）
        if (!activeFrames.contains(frameId)) {
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }

        // 如果在 pinned 中，转入 LRU
        if (pinnedFrames.contains(frameId)) {
            pinnedFrames.remove(frameId);
            LRUList.addLast(frameId);
            LRUHash.add(frameId);
        } else if (!LRUHash.contains(frameId)) {
            // 如果不在 pinned 也不在 LRU，说明是非法情况（一般不会发生，但保证逻辑健壮性）
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }
    }

    // 返回总缓存页面数（pinned + unpinned）
    public int size() {
        return LRUList.size() + pinnedFrames.size();
    }
}
