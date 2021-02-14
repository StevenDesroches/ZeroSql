package com.github.stevendesroches.zerosql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ZeroSql {

    private boolean usable = false;
    private ZeroSqlFunctionsManager zeroSqlFunctionsManager;

    private HikariConfig hikariConfig;
    private HikariDataSource hikariDataSource;

    public boolean initialisation(String host, String port, String database, String username, String password) {
        hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.setAutoCommit(true);

        return openSourceConnection();
    }

    public ZeroSqlFunctionsManager getZeroSqlFunctionsManager() {
        return zeroSqlFunctionsManager;
    }

    private boolean openSourceConnection() {
        boolean result = false;
        if (hikariConfig != null) {
            hikariDataSource = new HikariDataSource(hikariConfig);
            zeroSqlFunctionsManager = new ZeroSqlFunctionsManager(hikariDataSource);
            result = true;
        }
        return result;
    }

    public void closeSourceConnection() {
        if (hikariDataSource != null && !hikariDataSource.isClosed()) {
            hikariDataSource.close();
            hikariDataSource = null;
            zeroSqlFunctionsManager = null;
        }
    }


}
