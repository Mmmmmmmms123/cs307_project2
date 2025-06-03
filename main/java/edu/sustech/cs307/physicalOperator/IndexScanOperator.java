package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.List;

public class IndexScanOperator implements PhysicalOperator {

    private final DBManager dbManager;
    private final String tableName;
    private final Value targetValue;

    private final Index index; // 关键：面向接口编程！

    private final TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private final List<RID> matchedRIDs = new ArrayList<>();
    private int currentIndex = 0;
    private Record currentRecord;
    private boolean isOpen = false;

    public IndexScanOperator(DBManager dbManager, String tableName, String columnName,
                             Value targetValue, Index index) throws DBException {
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.targetValue = targetValue;
        this.index = index;
        this.tableMeta = dbManager.getMetaManager().getTable(tableName);
    }

    @Override
    public void Begin() throws DBException {
        RID rid = index.EqualTo(targetValue); // 使用统一接口
        if (rid != null) {
            matchedRIDs.add(rid);
        }

        fileHandle = dbManager.getRecordManager().OpenFile(tableName);
        currentIndex = 0;
        isOpen = true;
    }

    @Override
    public boolean hasNext() {
        return isOpen && currentIndex < matchedRIDs.size();
    }

    @Override
    public void Next() {
        if (!hasNext()) {
            currentRecord = null;
            return;
        }
        try {
            RID rid = matchedRIDs.get(currentIndex++);
            currentRecord = fileHandle.GetRecord(rid);
            fileHandle.UnpinPageHandle(rid.pageNum, false);
        } catch (DBException e) {
            e.printStackTrace();
            currentRecord = null;
        }
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentRecord == null) return null;
        return new TableTuple(tableName, tableMeta, currentRecord, matchedRIDs.get(currentIndex - 1));
    }

    @Override
    public void Close() {
        if (!isOpen) return;
        try {
            dbManager.getRecordManager().CloseFile(fileHandle);
        } catch (DBException e) {
            e.printStackTrace();
        }
        matchedRIDs.clear();
        currentIndex = 0;
        currentRecord = null;
        isOpen = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta.columns_list;
    }
}