package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;

public class LogicalSelectOperator extends LogicalOperator {
    private final LogicalOperator child;
    private final String columnName;
    private final Expression targetValue;

    public LogicalSelectOperator(LogicalOperator child, String columnName, Expression targetValue) {
        super(List.of(child)); // 有一个子算子：输入表
        this.child = child;
        this.columnName = columnName;
        this.targetValue = targetValue;
    }

    public LogicalOperator getChild() {
        return child;
    }

    public String getColumnName() {
        return columnName;
    }

    public Expression getTargetValue() {
        return targetValue;
    }

    @Override
    public String toString() {
        return "LogicalSelectOperator(column=" + columnName + ", value=" + targetValue + ")";
    }
}
