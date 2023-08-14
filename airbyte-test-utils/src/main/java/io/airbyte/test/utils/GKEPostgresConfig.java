/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import io.airbyte.db.Database;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.test.utils.AcceptanceTestHarness.Type;
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
 * {@link AcceptanceTestHarness} on GKE. We launch 2 postgres databases in GKE as pods which act as
 * source and destination and the tests run against them. In order to allow the test instance to
 * connect to these databases we use port forwarding Refer
 * tools/bin/gke-kube-acceptance-test/acceptance_test_kube_gke.sh for more info
 */
class GKEPostgresConfig {

  // NOTE: these two hosts refer to services named `acceptance-test-postgres-[source|destination]-svc`
  // in the `acceptance-tests` namespace, running in the same cluster as the check/discover/sync
  // workers.
  //
  // The namespace here needs to be in sync with the namespaces created in
  // tools/bin/gke-kube-acceptance-test/acceptance_test_kube_gke.sh.
  private static final String SOURCE_HOST = "acceptance-test-postgres-source-svc.acceptance-tests.svc.cluster.local";
  private static final String DESTINATION_HOST = "acceptance-test-postgres-destination-svc.acceptance-tests.svc.cluster.local";
  private static final Integer PORT = 5432;
  private static final String USERNAME = "postgresadmin";
  private static final String DB = "postgresdb";

  static Map<Object, Object> dbConfig(final Type connectorType, final String password, final boolean withSchema) {
    final Map<Object, Object> dbConfig = new HashMap<>();
    dbConfig.put(JdbcUtils.HOST_KEY, connectorType == Type.SOURCE ? SOURCE_HOST : DESTINATION_HOST);
    dbConfig.put(JdbcUtils.PASSWORD_KEY, password == null ? "**********" : password);

    dbConfig.put(JdbcUtils.PORT_KEY, PORT);
    dbConfig.put(JdbcUtils.DATABASE_KEY, DB);
    dbConfig.put(JdbcUtils.USERNAME_KEY, USERNAME);
    dbConfig.put(JdbcUtils.JDBC_URL_PARAMS, "connectTimeout=60");

    if (withSchema) {
      dbConfig.put(JdbcUtils.SCHEMA_KEY, "public");
    }

    return dbConfig;
  }

  static DataSource getDestinationDataSource(final String password) {
    // Note: we set the connection timeout to 30s. The underlying Hikari default is also 30s --
    // https://github.com/brettwooldridge/HikariCP#frequently-used -- but our DataSourceFactory
    // overrides that to MAX_INTEGER unless we explicitly specify it.
    return DataSourceFactory.create(USERNAME, password, DatabaseDriver.POSTGRESQL.getDriverClassName(),
        "jdbc:postgresql://localhost:4000/postgresdb", Map.of(PGProperty.CONNECT_TIMEOUT.getName(), "60"));
  }

  static DataSource getSourceDataSource(final String password) {
    // Note: we set the connection timeout to 30s. The underlying Hikari default is also 30s --
    // https://github.com/brettwooldridge/HikariCP#frequently-used -- but our DataSourceFactory
    // overrides that to MAX_INTEGER unless we explicitly specify it.
    return DataSourceFactory.create(USERNAME, password, DatabaseDriver.POSTGRESQL.getDriverClassName(),
        "jdbc:postgresql://localhost:2000/postgresdb", Map.of(PGProperty.CONNECT_TIMEOUT.getName(), "60"));
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
