package com.zxm.migrate.utils;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;

/**
 * @author Jason
 */
public class DatabaseUtil {

    public static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL_SUFFIX = "?useUnicode=true&charsetEncoding=utf8";
    private static final String URL_PREFIX = "jdbc:mysql://";
    public static final String SQL = "SELECT * FROM ";

    public static Connection getFromConnection(String fromIp, String fromPort, String fromUsername, String fromPassword, String fromDb) {

        Connection fromConn = null;

        try {
            Class.forName(DRIVER);
            if (fromIp != null && fromPort != null && fromUsername != null && fromPassword != null && fromDb != null) {
                String fromUrl = URL_PREFIX + fromIp + ":" + fromPort + "/" + fromDb + URL_SUFFIX;
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassName(DRIVER);
                ds.setUrl(fromUrl);
                ds.setUsername(fromUsername);
                ds.setPassword(fromPassword);

                fromConn = ds.getConnection();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return fromConn;
    }

    public static Connection getToConnection(String toIp, String toPort, String toUsername, String toPassword, String toDb) {

        Connection toConn = null;
        try {
            Class.forName(DRIVER);
            if (toIp != null && toPort != null && toUsername != null && toPassword != null && toDb != null) {
                String toUrl = URL_PREFIX + toIp + ":" + toPort + "/" + toDb + URL_SUFFIX;
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassName(DRIVER);
                ds.setUrl(toUrl);
                ds.setUsername(toUsername);
                ds.setPassword(toPassword);
                toConn = ds.getConnection();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return toConn;
    }


    public static ArrayList<String> getColumnTypes(Connection toConn, String toTable, ArrayList<String> toFieldList) {

        ArrayList<String> colTYpeList = new ArrayList<>();
        String sqlCmd = "SELECT * FROM ";

        try {
            String sql = sqlCmd + toTable;
            PreparedStatement pstm = toConn.prepareStatement(sql);
            ResultSetMetaData metaData = pstm.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (String s : toFieldList) {

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (columnName.equals(s)) {
                        colTYpeList.add(metaData.getColumnTypeName(i));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return colTYpeList;
    }

    public static Integer getLineCount(Connection fromConn, String fromTable) {

        Integer count = 0;
        try {
            String sql = "SELECT COUNT(1) FROM " + fromTable;
            Statement statement = fromConn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                count = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static String getInsertSql(ArrayList<String> toFieldList, String toTable, Integer colNum, Integer insertNum) {

        String insertSql = null;
        if (toTable != null && colNum != null && insertNum != null) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < colNum - 1; i++) {
                sb.append("?,");
            }
            String sqlTmp = sb.append("?)").toString();
            StringBuilder sqlCmd = new StringBuilder("INSERT INTO " + toTable);
            sqlCmd.append(" (").append(toFieldList.get(0));
            for (int i = 1; i < toFieldList.size() - 1; i++) {
                sqlCmd.append(",").append(toFieldList.get(i));
            }
            sqlCmd.append(",").append(toFieldList.get(colNum-1)).append(")").append(" VALUES ").append(sqlTmp).append(",");

            for (int i = 0; i <= insertNum - 3; i++) {
                sqlCmd.append(sqlTmp).append(",");
            }
            sqlCmd.append(sqlTmp);
            insertSql = sqlCmd.toString();
        }
        return insertSql;
    }


    public static String getRestInsertSql(ArrayList<String> toFieldList, String toTable, Integer colNum) {

        String insertSql = null;
        if (toTable != null && colNum != null) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < colNum - 1; i++) {
                sb.append("?,");
            }
            String sqlTmp = sb.append("?)").toString();
            StringBuilder sqlCmd = new StringBuilder("INSERT INTO " + toTable);
            sqlCmd.append(" (").append(toFieldList.get(0));
            for (int i = 1; i < toFieldList.size() - 1; i++) {
                sqlCmd.append(",").append(toFieldList.get(i));
            }

            sqlCmd.append(",").append(toFieldList.get(colNum - 1)).append(")").append(" VALUES ").append(sqlTmp);
            insertSql = sqlCmd.toString();
        }
        return insertSql;
    }

    public static void truncateTable(Connection toConn, String toTable) {

        String sql = "TRUNCATE " + toTable;

        try {
            Statement state = toConn.createStatement();
            state.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
