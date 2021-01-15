package com.github.stevendesroches.zerosql;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.List;

public class Functions {
    private final HikariDataSource dataSource;

    public Functions(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    private boolean basicExecuteStatement(String sql) {
        boolean result = false;
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            conn = dataSource.getConnection();
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            result = true;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private Row rowExecuteStatement(String sql) {
        Row result = null;
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultSetMetaData = null;
        try {
            conn = dataSource.getConnection();
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            resultSetMetaData = resultSet.getMetaData();

            if (resultSet.first()) {
                result = new Row();
                for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                    result.add(resultSetMetaData.getColumnName(i), resultSet.getObject(i), resultSetMetaData.getColumnTypeName(i));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                resultSetMetaData = null;
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private List<Row> rowListExecuteStatement(String sql) {
        List<Row> result = null;
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultSetMetaData = null;
        try {
            conn = dataSource.getConnection();
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            resultSetMetaData = resultSet.getMetaData();

            if (resultSet.next()) {
                Row currentRow = new Row();
                for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                    currentRow.add(resultSetMetaData.getColumnName(i), resultSet.getObject(i), resultSetMetaData.getColumnTypeName(i));
                }
                result.add(currentRow);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                resultSetMetaData = null;
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public boolean createDb(String dbName) {
        return createDb(dbName, null, null);
    }

    public boolean createDb(String dbName, String charSet) {
        return createDb(dbName, charSet, null);
    }

    public boolean createDb(String dbName, String charSet, String collation) {
        String sql = "CREATE DATABASE IF NOT EXISTS `" + dbName + "`";
        if (charSet != null) {
            sql += " CHARACTER SET " + charSet;
        }
        if (collation != null) {
            sql += " COLLATE " + collation;
        }
        sql += ";";
        return basicExecuteStatement(sql);
    }

    public boolean createTable(String name, String[] columns) {
        String sql = "CREATE TABLE `" + name + "` (id int(11) NOT NULL," + String.join(",", columns);
        sql += " PRIMARY KEY (id)";
        sql += " )";
        sql += ";";
        return basicExecuteStatement(sql);
    }

    public boolean createTable(String name, String[] columns, String primaryKey) {
        String sql = "CREATE TABLE `" + name + "` (" + String.join(",", columns);
        sql += " PRIMARY KEY (" + primaryKey + ")";
        sql += " )";
        sql += ";";
        return basicExecuteStatement(sql);
    }

    public boolean insert(String table, String[] columns, String[] values) {
        String sql = "INSERT INTO `" + table + "`";
        sql += "(" + String.join(",", columns) + ")";
        sql += " VALUES(" + String.join(",", values) + ")";
        sql += ";";
        return basicExecuteStatement(sql);
    }

    public boolean delete(String table, String[] where) {
        String sql = "DELETE FROM `" + table + "`";
        sql += " WHERE " + String.join(",", where);
        sql += ";";
        return basicExecuteStatement(sql);
    }

    public boolean drop(String table) {
        String sql = "DROP TABLE IF EXIST `" + table + "`;";
        return basicExecuteStatement(sql);
    }

    public Row get(String table, String[] columns, String[] where) {
        String sql = "SELECT " + String.join(",", columns) + " FROM " + table + " WHERE " + String.join(",", where) + ";";
        return rowExecuteStatement(sql);
    }

    public List<Row> getAll(String table, String[] columns) {
        return getAll(table, columns, null);
    }

    public List<Row> getAll(String table, String[] columns, String[] where) {
        String sql = "SELECT " + String.join(",", columns) + " FROM " + table;
        if (where != null) {
            sql += " WHERE " + String.join(",", where) + ";";
        }
        return rowListExecuteStatement(sql);
    }
}
