package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.logicalOperator.LogicalOperator;

import net.sf.jsqlparser.statement.ExplainStatement;

import net.sf.jsqlparser.statement.Statement;
import org.pmw.tinylog.Logger;

public class ExplainExecutor implements DMLExecutor {

    private final ExplainStatement explainStatement;
    private final DBManager dbManager;

    public ExplainExecutor(ExplainStatement explainStatement, DBManager dbManager) {
        this.explainStatement = explainStatement;
        this.dbManager = dbManager;
    }



    @Override
    public void execute() throws DBException {
        Statement stmtToExplain = explainStatement.getStatement();
        LogicalOperator logicalPlan = LogicalPlanner.resolveAndPlan(dbManager, stmtToExplain.toString());

        if (logicalPlan == null) {
            throw new DBException(ExceptionTypes.NOT_SUPPORTED_OPERATION);
        }

        String planStr = logicalPlan.toString();

        Logger.info(planStr);
    }


}
