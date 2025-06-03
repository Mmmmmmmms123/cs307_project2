package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;

import java.util.ArrayList;

public class SelectOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final String columnName;
    private final Value targetValue;
    private ColumnMeta targetColumn;
    private TableMeta tableMeta;
    private boolean isOpen = false;
    private Tuple currentTuple;

    public SelectOperator(PhysicalOperator child, String columnName, Value targetValue, DBManager dbManager) throws DBException {
        this.child = child;
        this.columnName = columnName;
        this.targetValue = targetValue;

        // 获取元数据
        if (child instanceof SeqScanOperator seq) {
            String tableName = seq.getFileHandle().getFilename();
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        } else {
            throw new DBException(ExceptionTypes.UNSUPPORTED_OPERATOR_TYPE);
        }

        for (ColumnMeta col : tableMeta.columns_list) {
            if (col.name.equals(columnName)) {
                targetColumn = col;
                break;
            }
        }

        if (targetColumn == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        isOpen = true;
        advance();
    }

    private void advance() throws DBException {
        currentTuple = null;
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple == null) continue;

            if (tuple instanceof TableTuple tableTuple) {
                TabCol targetTabCol = new TabCol(tableTuple.getTableName(), columnName);
                Value value = tableTuple.getValue(targetTabCol);

                if (value != null && value.equals(targetValue)) {
                    currentTuple = tuple;
                    break;
                }
            }
        }
    }

    @Override
    public void Next() throws DBException {
        advance();
    }

    @Override
    public boolean hasNext() {
        return isOpen && currentTuple != null;
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        isOpen = false;
        child.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}
