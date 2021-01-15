package com.github.stevendesroches.zerosql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ZeroSql {

    private boolean usable = false;
    private Functions functions;

    private HikariConfig hikariConfig;
    private HikariDataSource hikariDataSource;

    public void initialisation(String host, String port, String database, String username, String password) {
        hikariConfig.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.setAutoCommit(true);

        openSourceConnection();
    }

    public boolean openSourceConnection() {
        boolean result = false;
        if (hikariConfig != null) {
            hikariDataSource = new HikariDataSource(hikariConfig);
            functions = new Functions(hikariDataSource);
        }
        return result;
    }

    public void closeSourceConnection() {
        if (hikariDataSource != null && !hikariDataSource.isClosed()) {
            hikariDataSource.close();
            hikariDataSource = null;
            functions = null;
        }
    }


}
