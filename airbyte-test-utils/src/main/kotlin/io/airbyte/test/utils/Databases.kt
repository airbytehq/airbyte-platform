/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DataSourceFactory.create
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.SQLDialect
import org.junit.jupiter.api.Assertions
import org.testcontainers.containers.JdbcDatabaseContainer
import java.sql.SQLException
import java.util.stream.Collectors
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

/**
 * Collection of Acceptance test database queries to simplify test set up.
 */
object Databases {
  private const val COLUMN_NAME_DATA = "_airbyte_data"

  /**
   * Constructs a new [DataSource] using the provided configuration.
   *
   * @param container A JDBC Test Container instance.
   * @return The configured [DataSource].
   */
  @JvmStatic
  fun createDataSource(container: JdbcDatabaseContainer<*>): DataSource =
    create(
      container.username,
      container.password,
      container.driverClassName,
      container.jdbcUrl,
    )

  /**
   * Constructs a configured [DSLContext] instance using the provided configuration.
   *
   * @param dataSource A [DataSource] instance
   * @param dialect The SQL dialect to use with objects created from this context.
   * @return The configured [DSLContext].
   */
  @JvmStatic
  fun createDslContext(
    dataSource: DataSource,
    dialect: SQLDialect,
  ): DSLContext = create(dataSource, dialect)

  /**
   * List all tables, except system tables, in the given database.
   *
   * @param database to query
   * @return a set of Schema to Table name pairs.
   * @throws SQLException in case of database error.
   */
  @JvmStatic
  @Throws(SQLException::class)
  fun listAllTables(database: Database): Set<SchemaTableNamePair> =
    database.query { context: DSLContext ->
      val fetch =
        context.fetch(
          "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'",
        )
      fetch
        .stream()
        .map { record: Record ->
          val schemaName = record["schemaname"] as String
          val tableName = record["tablename"] as String
          SchemaTableNamePair(schemaName, tableName)
        }.collect(Collectors.toSet())
    }

  /**
   * Return all tables, except system tables, in the given database and schema.
   *
   * @param database to query.
   * @param schema to query.
   * @return a set of Schema to Table name pairs.
   * @throws SQLException in case of database error.
   */
  @Throws(SQLException::class)
  fun listAllTables(
    database: Database,
    schema: String,
  ): Set<SchemaTableNamePair> =
    database.query { context: DSLContext ->
      val fetch =
        context.fetch(
          "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE schemaname = '$schema'",
        )
      fetch
        .stream()
        .map { record: Record ->
          val schemaName = record["schemaname"] as String
          val tableName = record["tablename"] as String
          SchemaTableNamePair(schemaName, tableName)
        }.collect(Collectors.toSet())
    }

  @JvmStatic
  @Throws(SQLException::class)
  fun retrieveRecordsFromDatabase(
    database: Database,
    table: String?,
  ): List<JsonNode> =
    database
      .query { context: DSLContext ->
        context.fetch(
          String.format(
            "SELECT * FROM %s;",
            table,
          ),
        )
      }.stream()
      .map { obj: Record -> obj.intoMap() }
      .map { `object`: Map<String, Any>? -> Jsons.jsonNode(`object`) }
      .collect(Collectors.toList())

  @Throws(Exception::class)
  fun retrieveRawDestinationRecords(
    destDb: Database,
    schema: String,
    tableName: String,
  ): List<JsonNode> {
    val namePairs = listAllTables(destDb)

    log.debug { "schema: $schema table: $tableName" }
    log.debug { "source tables count: ${namePairs.size}" }
    log.debug { "tables found in destination: ${namePairs.joinToString(", ") { it.getFullyQualifiedTableName() }}" }
    val rawStreamName = String.format("_airbyte_raw_%s%s", AcceptanceTestHarness.OUTPUT_STREAM_PREFIX, tableName.replace(".", "_"))
    val rawTablePair = SchemaTableNamePair(schema, rawStreamName)
    Assertions.assertTrue(
      namePairs.contains(rawTablePair),
      "can't find a non-normalized version (raw) of " + rawTablePair.getFullyQualifiedTableName(),
    )

    return retrieveDestinationRecords(destDb, rawTablePair.getFullyQualifiedTableName())
  }

  @JvmStatic
  @Throws(SQLException::class)
  fun retrieveDestinationRecords(
    database: Database,
    table: String?,
  ): List<JsonNode> =
    database
      .query { context: DSLContext ->
        context.fetch(
          String.format(
            "SELECT * FROM %s;",
            table,
          ),
        )
      }.stream()
      .map { obj: Record -> obj.intoMap() }
      .map { r: Map<String, Any> -> r[COLUMN_NAME_DATA] }
      .map { f: Any? -> f as JSONB? }
      .map { obj: JSONB? -> obj!!.data() }
      .map { jsonString: String? -> Jsons.deserialize(jsonString!!) }
      .map { `object`: JsonNode? -> Jsons.jsonNode(`object`) }
      .collect(Collectors.toList())
}
