package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;

public class ShowTableExecutor implements DMLExecutor {
    DBManager dbManager;
    ShowTablesStatement showTableStatement;
    public ShowTableExecutor(DBManager dbManager,ShowTablesStatement showTablesStatement) {
        this.dbManager = dbManager;
        this.showTableStatement = showTablesStatement;
    }
    @Override
    public void execute() throws DBException {
        dbManager.showTables();
    }

}
