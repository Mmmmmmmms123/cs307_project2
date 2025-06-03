package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;

import java.util.*;

public class InMemoryIndexScanOperator implements PhysicalOperator {

    private final InMemoryOrderedIndex index;
    private final DBManager dbManager;
    private final String tableName;
    private final TableMeta tableMeta;
    private final Value targetValue; // 用于 EqualTo 查询
    private final List<RID> matchedRIDs = new ArrayList<>();

    private int currentIndex = 0;
    private Record currentRecord;
    private RecordFileHandle fileHandle;
    private boolean isOpen = false;

    public InMemoryIndexScanOperator(InMemoryOrderedIndex index, DBManager dbManager,
                                     String tableName, Value targetValue) throws DBException {
        this.index = index;
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        this.targetValue = targetValue;
    }

    @Override
    public void Begin() throws DBException {
        RID rid = index.EqualTo(targetValue);
        if (rid != null) {
            matchedRIDs.add(rid);
        }
        this.fileHandle = dbManager.getRecordManager().OpenFile(tableName);
        this.currentIndex = 0;
        this.isOpen = true;
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
