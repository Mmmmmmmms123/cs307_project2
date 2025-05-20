package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.drop.Drop;
import org.pmw.tinylog.Logger;


public class DropTableExecutor implements DMLExecutor{
    private final DBManager dbManager;
    private final Drop dropStatement;
    public DropTableExecutor(DBManager dbManager,Drop dropStatement){
        this.dbManager=dbManager;
        this.dropStatement=dropStatement;
    }

    @Override
    public void execute() throws DBException {
        String tableName = dropStatement.getName().getName();
//        MetaManager metaManager = dbManager.getMetaManager();
//        DiskManager diskManager = dbManager.getDiskManager();
//        BufferPool bufferPool = dbManager.getBufferPool();
//
//        dbManager.getBufferPool().DeleteAllPages(tableName);
//        metaManager.dropTable(tableName);
//        diskManager.DeleteFile(tableName);
        dbManager.dropTable(tableName);
        Logger.info("Table '{}' dropped successfully.", tableName);
    }

}
