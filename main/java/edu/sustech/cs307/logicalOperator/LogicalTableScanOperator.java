package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

public class LogicalTableScanOperator extends LogicalOperator {
    private final String tableName;
    private final DBManager dbManager;
    private Expression whereCondition; // 新增：用来识别跳转 顺序/b+树
    public LogicalTableScanOperator(String tableName, DBManager dbManager) throws DBException {
        super(Collections.emptyList()); // TableScan 没有子节点
        this.tableName = tableName;
        this.dbManager = dbManager;
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
    }

    public void setWhereCondition(Expression condition) {
        this.whereCondition = condition;
    }

    public Expression getWhereCondition() {
        return whereCondition;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "TableScanOperator(table=" + tableName + ")";
    }
}
