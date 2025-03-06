/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static io.airbyte.test.utils.AcceptanceTestHarness.OUTPUT_STREAM_PREFIX;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Collection of Acceptance test database queries to simplify test set up.
 */
public class Databases {

  private static final String COLUMN_NAME_DATA = "_airbyte_data";

  /**
   * Constructs a new {@link DataSource} using the provided configuration.
   *
   * @param container A JDBC Test Container instance.
   * @return The configured {@link DataSource}.
   */
  public static DataSource createDataSource(final JdbcDatabaseContainer container) {
    return DataSourceFactory.create(container.getUsername(),
        container.getPassword(),
        container.getDriverClassName(),
        container.getJdbcUrl());
  }

  /**
   * Constructs a configured {@link DSLContext} instance using the provided configuration.
   *
   * @param dataSource A {@link DataSource} instance
   * @param dialect The SQL dialect to use with objects created from this context.
   * @return The configured {@link DSLContext}.
   */
  public static DSLContext createDslContext(final DataSource dataSource, final SQLDialect dialect) {
    return DSLContextFactory.create(dataSource, dialect);
  }

  /**
   * List all tables, except system tables, in the given database.
   *
   * @param database to query
   * @return a set of Schema to Table name pairs.
   * @throws SQLException in case of database error.
   */
  public static Set<SchemaTableNamePair> listAllTables(final Database database) throws SQLException {
    return database.query(
        context -> {
          final Result<Record> fetch =
              context.fetch(
                  "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'");
          return fetch.stream()
              .map(record -> {
                final var schemaName = (String) record.get("schemaname");
                final var tableName = (String) record.get("tablename");
                return new SchemaTableNamePair(schemaName, tableName);
              })
              .collect(Collectors.toSet());
        });
  }

  /**
   * Return all tables, except system tables, in the given database and schema.
   *
   * @param database to query.
   * @param schema to query.
   * @return a set of Schema to Table name pairs.
   * @throws SQLException in case of database error.
   */
  public static Set<SchemaTableNamePair> listAllTables(final Database database, final String schema) throws SQLException {
    return database.query(
        context -> {
          final Result<Record> fetch =
              context.fetch(
                  "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE schemaname = '" + schema + "'");
          return fetch.stream()
              .map(record -> {
                final var schemaName = (String) record.get("schemaname");
                final var tableName = (String) record.get("tablename");
                return new SchemaTableNamePair(schemaName, tableName);
              })
              .collect(Collectors.toSet());
        });
  }

  public static List<JsonNode> retrieveRecordsFromDatabase(final Database database, final String table) throws SQLException {
    return database.query(context -> context.fetch(String.format("SELECT * FROM %s;", table)))
        .stream()
        .map(Record::intoMap)
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

  public static List<JsonNode> retrieveRawDestinationRecords(final Database destDb, final String schema, final String tableName) throws Exception {
    final Set<SchemaTableNamePair> namePairs = Databases.listAllTables(destDb);

    final String rawStreamName = String.format("_airbyte_raw_%s%s", OUTPUT_STREAM_PREFIX, tableName.replace(".", "_"));
    final SchemaTableNamePair rawTablePair = new SchemaTableNamePair(schema, rawStreamName);
    assertTrue(namePairs.contains(rawTablePair), "can't find a non-normalized version (raw) of " + rawTablePair.getFullyQualifiedTableName());

    return retrieveDestinationRecords(destDb, rawTablePair.getFullyQualifiedTableName());
  }

  public static List<JsonNode> retrieveDestinationRecords(final Database database, final String table) throws SQLException {
    return database.query(context -> context.fetch(String.format("SELECT * FROM %s;", table)))
        .stream()
        .map(Record::intoMap)
        .map(r -> r.get(COLUMN_NAME_DATA))
        .map(f -> (JSONB) f)
        .map(JSONB::data)
        .map(Jsons::deserialize)
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

}
