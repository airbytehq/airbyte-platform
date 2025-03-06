/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import io.airbyte.db.Database;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.postgresql.PGProperty;

/**
 * This class is used to provide information related to the test databases for running the
 * {@link AcceptanceTestHarness} on GKE.
 */
class GKEPostgresConfig {

  private static final Integer PORT = 5432;

  static Map<Object, Object> dbConfig(final String password,
                                      final boolean withSchema,
                                      String username,
                                      String cloudSqlInstanceIp,
                                      String databaseName) {
    final Map<Object, Object> dbConfig = new HashMap<>();
    dbConfig.put(JdbcUtils.HOST_KEY, cloudSqlInstanceIp);
    dbConfig.put(JdbcUtils.PASSWORD_KEY, password == null ? "**********" : password);

    dbConfig.put(JdbcUtils.PORT_KEY, PORT);
    dbConfig.put(JdbcUtils.DATABASE_KEY, databaseName);
    dbConfig.put(JdbcUtils.USERNAME_KEY, username);
    dbConfig.put(JdbcUtils.JDBC_URL_PARAMS, "connectTimeout=60");

    if (withSchema) {
      dbConfig.put(JdbcUtils.SCHEMA_KEY, "public");
    }

    return dbConfig;
  }

  static DataSource getDataSource(final String username, final String password, String cloudSqlInstanceIp, String databaseName) {
    return DataSourceFactory.create(username, password, DatabaseDriver.POSTGRESQL.getDriverClassName(),
        "jdbc:postgresql://" + cloudSqlInstanceIp + ":5432/" + databaseName, Map.of(PGProperty.CONNECT_TIMEOUT.getName(), "60"));
  }

  static void runSqlScript(final Path scriptFilePath, final Database db) throws SQLException, IOException {
    final StringBuilder query = new StringBuilder();
    for (final String line : java.nio.file.Files.readAllLines(scriptFilePath, StandardCharsets.UTF_8)) {
      if (line != null && !line.isEmpty()) {
        query.append(line);
      }
    }
    db.query(context -> context.execute(query.toString()));
  }

}
