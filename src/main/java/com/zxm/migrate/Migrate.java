package com.zxm.migrate;


import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Jason
 */
public class Migrate implements Runnable {

    private static int i = 0;
    public static final Integer COMMIT_SUM = 500;
    private CopyOnWriteArrayList<String> lines;
    private PreparedStatement toStmt;
    private PreparedStatement restToStmt;
    private Connection toConn;
    private Integer colNum;
    private ArrayList<String> columnTypes;
    private Integer curLineCount;

    public Migrate(CopyOnWriteArrayList<String> lines, PreparedStatement toStmt, PreparedStatement restToStmt, Connection toConn, Integer colNum, ArrayList<String> columnTypes, Integer curLineCount) {

        this.lines = lines;
        this.toStmt = toStmt;
        this.restToStmt = restToStmt;
        this.toConn = toConn;
        this.colNum = colNum;
        this.columnTypes = columnTypes;
        this.curLineCount = curLineCount;
    }

    @Override
    public void run() {

        try {
            toConn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        write(toConn, lines, toStmt, colNum, columnTypes, curLineCount, restToStmt);

        try {
            toConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static synchronized void write(Connection toConn, CopyOnWriteArrayList<String> lines, PreparedStatement toStmt, Integer colNum, ArrayList<String> columnTypes, Integer curLineCount, PreparedStatement restToStmt) {

        try {
            Iterator<String> iterator = lines.iterator();
            int n = 0;
            while (iterator.hasNext()) {

                i++;
                int j;
                if (curLineCount - n * Start.INSERT_NUM > Start.INSERT_NUM) {
                    for (int k = 1; k <= Start.INSERT_NUM; k++) {

                        if (iterator.hasNext()) {

                            String[] cols = iterator.next().split("\t\t");

                            for (j = 0; j < colNum; j++) {

                                if (cols[j] != null && !"null".equalsIgnoreCase(cols[j])) {

                                    if ("BIT".equalsIgnoreCase(columnTypes.get(j))) {

                                        toStmt.setInt((j + 1) + (k - 1) * colNum, Integer.parseInt(cols[j]));
                                    } else {
                                        toStmt.setString((j + 1) + (k - 1) * colNum, cols[j]);
                                    }
                                } else {
                                    toStmt.setString((j + 1) + (k - 1) * colNum, null);
                                }
                            }
                        }
                    }
                    toStmt.executeUpdate();
                    n++;
                } else {
                    String[] cols = iterator.next().split("\t\t");

                    for (j = 0; j < colNum; j++) {

                        if (cols[j] != null && !"null".equalsIgnoreCase(cols[j])) {
                            if ("BIT".equalsIgnoreCase(columnTypes.get(j))) {
                                restToStmt.setInt((j + 1), Integer.parseInt(cols[j]));
                            } else {
                                restToStmt.setString((j + 1), cols[j]);
                            }
                        } else {
                            restToStmt.setString((j + 1), null);
                        }
                    }
                    restToStmt.executeUpdate();
                }
                if (i >= COMMIT_SUM) {
                    toConn.commit();
                    i = 0;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (toConn != null) {
                    toConn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

}
