/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import io.airbyte.db.Database
import io.airbyte.db.factory.DataSourceFactory.create
import io.airbyte.db.factory.DatabaseDriver
import io.airbyte.db.jdbc.JdbcUtils
import org.jooq.DSLContext
import org.postgresql.PGProperty
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException
import javax.sql.DataSource

/**
 * This class is used to provide information related to the test databases for running the
 * [AcceptanceTestHarness] on GKE.
 */
internal object GKEPostgresConfig {
  private const val PORT = 5432

  @JvmStatic
  fun dbConfig(
    password: String?,
    withSchema: Boolean,
    username: String,
    cloudSqlInstanceIp: String,
    databaseName: String?,
  ): Map<Any, Any?> {
    val dbConfig: MutableMap<Any, Any?> = HashMap()
    dbConfig[JdbcUtils.HOST_KEY] = cloudSqlInstanceIp
    dbConfig[JdbcUtils.PASSWORD_KEY] = password ?: "**********"

    dbConfig[JdbcUtils.PORT_KEY] = PORT
    dbConfig[JdbcUtils.DATABASE_KEY] = databaseName
    dbConfig[JdbcUtils.USERNAME_KEY] = username
    dbConfig[JdbcUtils.JDBC_URL_PARAMS] = "connectTimeout=60"

    if (withSchema) {
      dbConfig[JdbcUtils.SCHEMA_KEY] = "public"
    }

    return dbConfig
  }

  @JvmStatic
  fun getDataSource(
    username: String,
    password: String,
    cloudSqlInstanceIp: String,
    databaseName: String,
  ): DataSource =
    create(
      username,
      password,
      DatabaseDriver.POSTGRESQL.driverClassName,
      "jdbc:postgresql://$cloudSqlInstanceIp:5432/$databaseName",
      java.util.Map.of(PGProperty.CONNECT_TIMEOUT.getName(), "60"),
    )

  @JvmStatic
  @Throws(SQLException::class, IOException::class)
  fun runSqlScript(
    scriptFilePath: Path,
    db: Database,
  ) {
    val query = StringBuilder()
    for (line in Files.readAllLines(scriptFilePath, StandardCharsets.UTF_8)) {
      if (line != null && !line.isEmpty()) {
        query.append(line)
      }
    }
    db.query { context: DSLContext -> context.execute(query.toString()) }
  }
}
