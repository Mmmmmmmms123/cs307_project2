package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

import java.util.Map;

public interface IndexManager {
    // 获取指定表和列的索引实例
    Index getIndex(String tableName, String columnName) throws DBException;

    // 创建索引（如果不存在）
    void createIndex(String tableName, String columnName) throws DBException;

    // 删除索引
    void dropIndex(String tableName, String columnName) throws DBException;

    // 获取所有索引的元信息
    Map<String, Map<String, Index>> getAllIndexes() throws DBException;
}
