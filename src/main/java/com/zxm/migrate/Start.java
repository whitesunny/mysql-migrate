package com.zxm.migrate;


import com.zxm.migrate.utils.DatabaseUtil;
import com.zxm.migrate.utils.FileUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.*;

/**
 * @author Jason
 */
public class Start {

    private static ExecutorService executor = new ThreadPoolExecutor(20, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));

    public static final Logger logger = Logger.getLogger(Start.class);

    public static Integer THREAD_NUM = 10;

    public static final Integer INSERT_NUM = 50;

    public static String fromIp;

    public static String fromPort;

    public static String fromUsername;

    public static String fromPassword;

    public static String fromDb;

    public static String fromTable;

    public static ArrayList<String> fromFieldList;

    public static Integer colNum;

    public static Connection fromConn;

    public static PreparedStatement fromStmt;

    public static String toIp;

    public static String toPort;

    public static String toUsername;

    public static String toPassword;

    public static String toDb;

    public static String toTable;

    public static ArrayList<String> toFieldList;

    public static Connection toConn;

    public static PreparedStatement toStmt;

    public static PreparedStatement restToStmt;

    public static ResultSet resultSet;

    public static LinkedHashMap<Integer, Object> tableProperties;

    public static String IS_TRUNCATE = "is_truncate";

    public static void main(String[] args) {

        logger.info("*** MYSQL MIGRATE ***");

        //获取数据库连接信息
//        String path = System.getProperty("user.dir");
//        LinkedHashMap<Integer,HashMap<String,String>> serverProperties = FileUtil.getServerProperties(path + "/server.xls");
        LinkedHashMap<Integer, HashMap<String, String>> serverProperties = FileUtil.getServerProperties("d:/migrate/server.xls");

        //获取映射配置文件信息
//        tableProperties = FileUtil.getTableProperties(path + "/table.xld");
        tableProperties = FileUtil.getTableProperties("d:/migrate/table.xls");

        //获取迁移配置文件信息
//        String migratePath = path + "/migrate.properties";
        String migratePath = "d:/migrate/migrate.properties";
        String isTruncate = FileUtil.readMigrateProperties(migratePath, IS_TRUNCATE);

        if (!tableProperties.isEmpty()) {

            for (int n = 1; n <= serverProperties.size(); n++) {

                HashMap<String, String> hashMap = serverProperties.get(n);

                for (int i = 1; i <= tableProperties.size(); i++) {

                    HashMap<String, Object> map = (HashMap<String, Object>) tableProperties.get(i);
                    String sourceSql = "SELECT ";
                    CopyOnWriteArrayList<String> lines;
                    try {
                        fromIp = hashMap.get("sourceIp");
                        fromPort = hashMap.get("sourcePort");
                        fromUsername = hashMap.get("sourceUsername");
                        fromPassword = hashMap.get("sourcePassword");
                        if (StringUtils.isEmpty(hashMap.get("sourceDb"))) {
                            if (!StringUtils.isEmpty((String) map.get("fromDb"))) {
                                fromDb = (String) map.get("fromDb");
                            } else {
                                throw new RuntimeException("table.xls parsing error");
                            }
                        } else {
                            if (StringUtils.isEmpty((String) map.get("fromDb"))) {
                                fromDb = hashMap.get("sourceDb");
                            } else {
                                fromDb = (String) map.get("fromDb");
                            }
                        }
                        fromTable = (String) map.get("fromTable");
                        fromFieldList = (ArrayList<String>) map.get("fromFieldList");
                        colNum = (Integer) map.get("colNum");

                        toIp = hashMap.get("targetIp");
                        toPort = hashMap.get("targetPort");
                        toUsername = hashMap.get("targetUsername");
                        toPassword = hashMap.get("targetPassword");
                        if (StringUtils.isEmpty(hashMap.get("targetDb"))) {

                            if (!StringUtils.isEmpty((String) map.get("toDb"))) {
                                toDb = (String) map.get("toDb");
                            } else {
                                throw new RuntimeException("table.xls parsing error");
                            }
                        } else {
                            if (StringUtils.isEmpty((String) map.get("toDb"))) {
                                toDb = hashMap.get("targetDb");
                            } else {
                                toDb = (String) map.get("toDb");
                            }
                        }
                        toTable = (String) map.get("toTable");
                        toFieldList = (ArrayList<String>) map.get("toFieldList");

                        sourceSql += fromFieldList.get(0);
                        for (int j = 1; j < fromFieldList.size(); j++) {
                            sourceSql += " , " + fromFieldList.get(j);
                        }
                        sourceSql += " FROM " + fromTable;
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("server.xls or table.xls parsing error");
                    }

                    try {
                        if (i == 1) {
                            logger.info(" Start the migrate from [" + fromIp + "] to [" + toIp + "]");
                        }
                        logger.info(" Start the migrate from [" + fromIp + "." + fromDb + "." + fromTable + "] to [" + toIp + "." + toDb + "." + toTable + "]");
                        fromConn = DatabaseUtil.getFromConnection(fromIp, fromPort, fromUsername, fromPassword, fromDb);
                        toConn = DatabaseUtil.getToConnection(toIp, toPort, toUsername, toPassword, toDb);

                        //写入数据之前,是否清理表数据
                        if ("1".equalsIgnoreCase(isTruncate)) {
                            DatabaseUtil.truncateTable(toConn, toTable);
                        }
                        Integer lineCount = DatabaseUtil.getLineCount(fromConn, fromTable);
                        ArrayList<String> columnTypes = DatabaseUtil.getColumnTypes(toConn, toTable, toFieldList);

                        String insertSql = DatabaseUtil.getInsertSql(toFieldList, toTable, colNum, INSERT_NUM);
                        String restInsertSql = DatabaseUtil.getRestInsertSql(toFieldList, toTable, colNum);

                        toStmt = toConn.prepareStatement(insertSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        restToStmt = toConn.prepareStatement(restInsertSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        toStmt.setFetchSize(Integer.MIN_VALUE);
                        restToStmt.setFetchSize(Integer.MIN_VALUE);
                        toConn.setAutoCommit(false);

                        int curLineCount;
                        if (lineCount < THREAD_NUM) {
                            curLineCount = lineCount;
                        } else {
                            curLineCount = lineCount / THREAD_NUM;
                        }
                        for (int k = 0; k <= THREAD_NUM; k++) {

                            lines = new CopyOnWriteArrayList<>();
                            int curLine = k * curLineCount;
                            if (k == THREAD_NUM) {
                                curLineCount = lineCount - THREAD_NUM * (lineCount / THREAD_NUM);
                            }
                            String sql = sourceSql + " LIMIT " + curLine + "," + curLineCount;
                            fromStmt = fromConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                            resultSet = fromStmt.executeQuery();
                            while (resultSet.next()) {
                                String line = resultSet.getString(1);
                                for (int j = 1; j < colNum; j++) {
                                    line += "\t\t" + resultSet.getString(j + 1);
                                }
                                lines.add(line);
                            }
                            executor.execute(new Migrate(lines, toStmt, restToStmt, toConn, colNum, columnTypes, curLineCount));
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    logger.info(" The migrate from [" + fromIp + "." + fromDb + "." + fromTable + "] to [" + toIp + "." + toDb + "." + toTable + "] completed");
                    if (i == tableProperties.size()) {
                        logger.info(" The migrate from [" + fromIp + "] to [" + toIp + "] completed");
                    }
                }
            }
        }
        //关闭线程池
        executor.shutdown();
    }
}
