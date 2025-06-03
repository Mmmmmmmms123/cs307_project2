package edu.sustech.cs307.physicalOperator;
/*
总结：SeqScanOperator 的核心作用
数据读取器	顺序扫描整个表，逐行读取元组
迭代器接口	提供 hasNext()/Next()/Current() 控制遍历过程
更新目标定位器	提供 RID，用于准确定位要更新的记录
通用性强	即使没有索引，任意表结构都能用它来访问
 */
import edu.sustech.cs307.meta.ColumnMeta;

import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.RecordPageHandle;
import edu.sustech.cs307.record.BitMap;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;

import java.util.ArrayList;

public class SeqScanOperator implements PhysicalOperator {
    //顺序扫描某个表的所有记录，一条条地读取 Record 并封装为 Tuple 供上层算子使用。
    private String tableName;
    private DBManager dbManager;
    private TableMeta tableMeta;//表元数据结构：记录表中所有结构信息
    private RecordFileHandle fileHandle;//文件句柄，用于读写页和记录。
    private Record currentRecord;//当前处理的记录

    private int currentPageNum;//页号
    private int currentSlotNum;//槽号
    /*
    页（Page）	数据存储的物理单位，通常为 4KB，每页存多个槽
    槽（Slot）	页内记录的位置，槽是否使用由 bitmap 标记
     */
    private int totalPages;
    private int recordsPerPage;//每页记录的槽数
    private boolean isOpen = false;//扫描是否开启

  //初始化+取信息操作（tableMeta）
    public SeqScanOperator(String tableName, DBManager dbManager) {
        this.tableName = tableName;
        this.dbManager = dbManager;
        try {
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        } catch (DBException e) {
            // Handle exception properly, maybe log or rethrow
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if (!isOpen)
            return false;
        try {
            // Check if current page and slot are valid, and if there are more records
            if (currentPageNum <= totalPages) {
                while (currentPageNum <= totalPages) {
                    RecordPageHandle pageHandle = fileHandle.FetchPageHandle(currentPageNum);
                    while (currentSlotNum < recordsPerPage) {
                        if (BitMap.isSet(pageHandle.bitmap, currentSlotNum)) {
                            return true; // Found next record（bitmap判断是否该槽位为有效记录）
                        }
                        currentSlotNum++;
                    }
                    currentPageNum++;
                    currentSlotNum = 0; // Reset slot num for new page
                }
            }
        } catch (DBException e) {
            e.printStackTrace(); // Handle exception properly
        }
        return false; // No more records
    }

    @Override
    public void Begin() throws DBException {
        try {
            fileHandle = dbManager.getRecordManager().OpenFile(tableName);
            totalPages = fileHandle.getFileHeader().getNumberOfPages();
            recordsPerPage = fileHandle.getFileHeader().getNumberOfRecordsPrePage();
            currentPageNum = 1; // Start from first page(P0是页头)
            currentSlotNum = 0; // Start from first slot
            isOpen = true;
        } catch (DBException e) {
            e.printStackTrace(); // Handle exception properly
            isOpen = false;
        }
    }

    @Override
    public void Next() {
        if (!isOpen)
            return;
        try {
            if (hasNext()) { // Advance to the next record
                RID rid = new RID(currentPageNum, currentSlotNum);//类似于坐标的东西
                currentRecord = fileHandle.GetRecord(rid);
                currentSlotNum++;
                if (currentSlotNum >= recordsPerPage) {
                    currentPageNum++;
                    currentSlotNum = 0;
                }
                // readonly
                fileHandle.UnpinPageHandle(currentPageNum, false);//缓存管理释放引用
            } else {
                currentRecord = null;
            }
        } catch (DBException e) {
            e.printStackTrace(); // Handle exception properly
            currentRecord = null;
        }
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentRecord == null) {
            return null;
        }
        return new TableTuple(tableName, tableMeta, currentRecord, new RID(this.currentPageNum, this.currentSlotNum - 1));
    }

    @Override
    public void Close() {
        if (!isOpen)
            return;
        try {
            dbManager.getRecordManager().CloseFile(fileHandle);
        } catch (DBException e) {
            e.printStackTrace(); // Handle exception properly
        }
        fileHandle = null;
        currentRecord = null;
        isOpen = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta.columns_list;
    }//提供输出的列结构

    public RecordFileHandle getFileHandle() {
        return fileHandle;
    }
}
