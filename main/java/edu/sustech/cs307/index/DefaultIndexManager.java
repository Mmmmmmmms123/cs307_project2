package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultIndexManager implements IndexManager {

    private final Map<String, Map<String, Index>> indexes = new HashMap<>();
    private final DBManager dbManager;

    public DefaultIndexManager(DBManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public Index getIndex(String tableName, String columnName) throws DBException {
        if (!indexes.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TABLE_DOES_NOT_EXIST);
        }
        Map<String, Index> tableIndexes = indexes.get(tableName);
        if (!tableIndexes.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.COLUMN_DOES_NOT_EXIST);
        }
        return tableIndexes.get(columnName);
    }

    @Override
    public void createIndex(String tableName, String columnName) throws DBException {
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        // 检查列是否存在
        ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
        if (columnMeta == null) {
            throw new DBException(ExceptionTypes.COLUMN_DOES_NOT_EXIST);
        }

        // 如果表还没有索引映射，则创建一个
        Map<String, Index> tableIndexes = indexes.computeIfAbsent(tableName, k -> new HashMap<>());

        // 判断使用哪种索引
        if (shouldUseBPlusTree(tableMeta, columnName)) {
            tableIndexes.put(columnName, new BPlusTreeIndex());
        } else {
            tableIndexes.put(columnName, new InMemoryOrderedIndex(getIndexPath(tableName, columnName)));
        }
    }

    @Override
    public void dropIndex(String tableName, String columnName) throws DBException {
        if (!indexes.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TABLE_DOES_NOT_EXIST);
        }
        Map<String, Index> tableIndexes = indexes.get(tableName);
        if (!tableIndexes.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.COLUMN_DOES_NOT_EXIST);
        }
        tableIndexes.remove(columnName);
    }

    @Override
    public Map<String, Map<String, Index>> getAllIndexes() throws DBException {
        return new HashMap<>(indexes); // 返回浅拷贝
    }

    /**
     * 判断是否应该使用 B+ 树索引
     */
    private boolean shouldUseBPlusTree(TableMeta tableMeta, String columnName) {
        // 如果列是主键或唯一键，则使用 B+ 树索引
        ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
        if (columnMeta != null && (columnMeta.isPrimaryKey || columnMeta.isUnique)) {
            return true;
        }
        // 如果没有统计信息，设都使用内存索引
        return false;
    }

    /**
     * 获取索引文件路径
     */
    private String getIndexPath(String tableName, String columnName) {
        return String.format("%s/%s/index_%s.idx",
                dbManager.getDiskManager().getCurrentDir(),
                tableName,
                columnName);
    }
}