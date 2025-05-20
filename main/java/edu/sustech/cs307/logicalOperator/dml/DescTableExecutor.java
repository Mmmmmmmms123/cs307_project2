package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.DescribeStatement;


public class DescTableExecutor implements DMLExecutor {
    private final DBManager dbManager;
    private final DescribeStatement describeStatement;

    public DescTableExecutor(DBManager dbManager, DescribeStatement describeStatement) {
        this.dbManager = dbManager;
        this.describeStatement = describeStatement;
    }

    @Override
    public void execute() {
        String tableName = describeStatement.getTable().getName();
        dbManager.descTable(tableName);
    }
}

