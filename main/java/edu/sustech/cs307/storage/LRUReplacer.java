package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    // pinned 页面不会被淘汰
    private final Set<Integer> pinnedFrames = new HashSet<>();

    // LRU 列表保存的是可淘汰的 frameId，最前面是最久没使用的
    private final Set<Integer> LRUHash = new HashSet<>();//保证性能
    private final LinkedList<Integer> LRUList = new LinkedList<>();//保证顺序

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    // 淘汰最近最久未使用、未被 pinned 的页面
    public int Victim() {
        if (LRUList.isEmpty()) return -1;
        int victim = LRUList.removeFirst();
        LRUHash.remove(victim);
        return victim;
    }

    // 页正在使用，不可淘汰，从 LRU 中移除
    public void Pin(int frameId) {
        // 如果已经存在就忽略（防止重复 Pin）
        if (pinnedFrames.contains(frameId) || LRUHash.contains(frameId)) return;

        if (size() >= maxSize) {
            throw new RuntimeException("REPLACER IS FULL");
        }

        pinnedFrames.add(frameId);
    }

    // 页不再使用，可参与淘汰，加入 LRU 尾部
    public void Unpin(int frameId) {
        // 如果已经是可淘汰的页，则不重复加入
        if (!pinnedFrames.contains(frameId) && !LRUHash.contains(frameId)) {
            LRUList.addLast(frameId);
            LRUHash.add(frameId);
        } else if (pinnedFrames.contains(frameId)) {
            pinnedFrames.remove(frameId);
            LRUList.addLast(frameId);
            LRUHash.add(frameId);
        }
    }


    // 返回总缓存页面数（pinned + unpinned）
    public int size() {
        return LRUList.size() + pinnedFrames.size();
    }
}