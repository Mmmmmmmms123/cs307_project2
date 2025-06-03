package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

public class LogicalDeleteOperator extends LogicalOperator {
    private final String tableName;
    private final Expression expressions;

    public LogicalDeleteOperator(LogicalOperator input, String tableName,Expression expression) {
        super(Collections.singletonList(input)); // 调用父类构造器，传入子节点列表
        this.tableName = tableName;
        this.expressions=expression;
    }

    public String getTableName() {
        return tableName;
    }
    public Expression getExpression() {
        return expressions;
    }

    @Override
    public String toString() {
        return "LogicalDeleteOperator(table=" + tableName + ", expression=" + expressions + ")\n ├── " + getChild();
    }

}