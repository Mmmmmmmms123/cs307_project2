package edu.sustech.cs307;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.index.IndexManager;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.physicalOperator.PhysicalOperator;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.RecordManager;
import edu.sustech.cs307.tuple.Tuple;

import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.TerminalBuilder;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class DBEntry {
    public static final String DB_NAME = "CS307-DB";
    // for now, we use 256 * 512 * 4096 bytes = 512MB as the pool size
    public static final int POOL_SIZE = 256 * 512;

    public static void printHelp() {
        Logger.info("Type 'exit' to exit the program.");
        Logger.info("Type 'help' to see this message again.");
    }

    public static void main(String[] args) throws DBException {
        Logger.getConfiguration().formatPattern("{date: HH:mm:ss.SSS} {level}: {message}").activate();

        Logger.info("Hello, This is CS307-DB!");
        Logger.info("Initializing...");
        DBManager dbManager ;
        try {
            //读取并加载磁盘管理器的元数据（例如每个数据文件的页数、偏移量等），通常是从磁盘上的某个元数据文件中加载。
            Map<String, Integer> disk_manager_meta = new HashMap<>(DiskManager.read_disk_manager_meta());
            /*
            创建 DiskManager 实例，它负责文件系统的底层管理，如页面读写、文件增删、磁盘同步等。
            依赖刚才加载的元数据进行恢复和构建内部状态。
             */
            DiskManager diskManager = new DiskManager(DB_NAME, disk_manager_meta);
            /*
            创建缓存池（Buffer Pool），用于缓存磁盘页，避免频繁 IO。
            通常具备 LRU 等替换策略，可以大幅提高性能。
            POOL_SIZE 是缓存的页数量上限。
             */
            BufferPool bufferPool = new BufferPool(POOL_SIZE, diskManager);
            /*
            构建记录管理器，负责表的记录插入、删除、扫描、更新等功能。
            它建立在 diskManager 和 bufferPool 基础上工作。
             */
            RecordManager recordManager = new RecordManager(diskManager, bufferPool);
            /*
            构建元数据管理器，负责管理表的模式（schema）、索引信息等。
            通常与数据库文件夹中的 meta 文件打交道。
             */
            MetaManager metaManager = new MetaManager(DB_NAME + "/meta");
            /*
            整合上述四个组件，创建统一的数据库管理器。
            DBManager 是系统对外暴露的主要接口，封装了对数据库的各种操作
             */
            IndexManager indexManager = new IndexManager() {
                @Override
                public Index getIndex(String tableName, String columnName) throws DBException {
                    return null;
                }

                @Override
                public void createIndex(String tableName, String columnName) throws DBException {

                }

                @Override
                public void dropIndex(String tableName, String columnName) throws DBException {

                }

                @Override
                public Map<String, Map<String, Index>> getAllIndexes() throws DBException {
                    return null;
                }
            };
            dbManager = new DBManager(diskManager, bufferPool, recordManager, metaManager,indexManager);
        } catch (DBException e) {
            Logger.error(e.getMessage());
            Logger.error("An error occurred during initializing. Exiting....");
            return;
        }

        /*
        这是一个典型的 REPL（Read-Eval-Print Loop）：
        Read：读取用户输入的 SQL 语句。
        Eval：将 SQL 转换成逻辑计划，再转换成物理计划，最终执行。
        Print：打印查询结果（使用 getRecordString() 等格式化输出）。
        Loop：循环直到用户输入 exit。
         */
        String sql = "";//存储当前用户输入的 SQL 语句。
        boolean running = true;//控制主循环是否继续运行。
        try {
            while (running) {
                try {
                    /*
                    使用LineReader 实现命令行输入；
                    TerminalBuilder.dumb(true) 表示使用简易终端，不启用高级功能；
                    显示提示符 CS307-DB> 并读取用户输入
                     */
                    LineReader scanner = LineReaderBuilder.builder()
                            .terminal(
                                    TerminalBuilder
                                            .builder()
                                            .dumb(true)
                                            .build()
                            )
                            .build();
                    Logger.info("CS307-DB> ");
                    sql = scanner.readLine();
                    if (sql.equalsIgnoreCase("exit")) {
                        running = false;
                        continue;
                    } else if (sql.equalsIgnoreCase("help")) {
                        printHelp();
                        //显示帮助信息
                        continue;
                    }
                } catch (Exception e) {
                    Logger.error(e.getMessage());
                    Logger.error("An error occurred. Exiting....");
                }
                try {
                    //逻辑计划阶段：解析 SQL；-->构建语法树、验证语义；-->生成逻辑操作符（如 Select, Project, Join 等）
                    LogicalOperator operator = LogicalPlanner.resolveAndPlan(dbManager, sql);
                    if (operator == null) {
                        continue;
                    }
                    //物理计划阶段：将逻辑操作符转换为物理执行算子（如 TableScan, NestedLoopJoin 等）。
                    PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, operator);
                    if (physicalOperator == null) {
                        Logger.info(operator);
                        continue;
                    }
                    // 表头上边框
                    Logger.info(getStartEndLine(physicalOperator.outputSchema().size(), true));
                    //列名
                    Logger.info(getHeaderString(physicalOperator.outputSchema()));
                    //分隔线
                    Logger.info(getSperator(physicalOperator.outputSchema().size()));
                    physicalOperator.Begin();
                    //逐行打印
                    while (physicalOperator.hasNext()) {
                        physicalOperator.Next();
                        Tuple tuple = physicalOperator.Current();
                        Logger.info(getRecordString(tuple));
                        Logger.info(getSperator(physicalOperator.outputSchema().size()));
                    }
                    //关闭操作符释放资源；
                    //强制刷新所有缓存页到磁盘（确保持久化）。
                    physicalOperator.Close();
                    dbManager.getBufferPool().FlushAllPages("");
                } catch (DBException e) {
                    Logger.error(e.getMessage());
                    Logger.error("An error occurred. Please try again.");
                    Logger.error(Arrays.toString(e.getStackTrace()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // persist the disk manager
            dbManager.getBufferPool().FlushAllPages("");
            Logger.error("Some error occurred. Exiting after persistdata...");
        }
    }

    //打印表头
    public static String getHeaderString(ArrayList<ColumnMeta> columnMetas) {
        StringBuilder header = new StringBuilder("|");
        for (var entry : columnMetas) {
            String tabcol = String.format("%s.%s", entry.tableName, entry.name);
            String centeredText = StringUtils.center(tabcol, 15, ' ');
            header.append(centeredText).append("|");
        }
        return header.toString();
    }

    /*
    将数据库元组（Tuple 对象）中的各个字段值格式化成一行字符串，
    适合以“表格”形式在终端中展示，每个字段用 | 分隔，并统一居中对齐，每列占 15 个字符宽度。
     */
    public static String getRecordString(Tuple tuple) throws DBException {
        StringBuilder tuple_string = new StringBuilder("|");
        for (var entry : tuple.getValues()) {
            String tabCol = String.format("%s", entry);
            String centeredText = StringUtils.center(tabCol, 15, ' ');
            tuple_string.append(centeredText).append("|");
        }
        return tuple_string.toString();
    }

    //生成表格的分隔线
    public static String getSperator(int width) {
        // ───────────────
        StringBuilder line = new StringBuilder("+");
        for (int i = 0; i < width; i++) {
            line.append("───────────────");
            line.append("+");
        }
        return line.toString();
    }


    /*
    生成表格的起始线或结束线
    通常用于在终端或控制台中以文本方式绘制表格
     */
    public static String getStartEndLine(int width, boolean header) {
        StringBuilder end_line;
        if (header) {
            end_line = new StringBuilder("┌");
        } else {
            end_line = new StringBuilder("└");
        }
        for (int i = 0; i < width; i++) {
            end_line.append("───────────────");
            if (header) {
                end_line.append("┐");
            } else {
                end_line.append("┘");
            }
        }
        return end_line.toString();
    }
}
