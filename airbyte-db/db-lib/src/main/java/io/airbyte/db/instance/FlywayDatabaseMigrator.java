/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.output.BaselineResult;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flyway migration.
 */
public class FlywayDatabaseMigrator implements DatabaseMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlywayDatabaseMigrator.class);

  /**
   * Set of schemas used to filter the schema dump generated via the jOOQ DDL.
   */
  private static final Set<String> SCHEMAS = Set.of("public");

  private final Database database;
  private final Flyway flyway;

  public FlywayDatabaseMigrator(final Database database, final Flyway flyway) {
    this.database = database;
    this.flyway = flyway;
  }

  @Override
  public MigrateResult migrate() {
    final MigrateResult result = flyway.migrate();
    result.warnings.forEach(LOGGER::warn);
    return result;
  }

  @Override
  public List<MigrationInfo> list() {
    final MigrationInfoService result = flyway.info();
    return Arrays.asList(result.all());
  }

  @Override
  public MigrationInfo getLatestMigration() {
    return flyway.info().current();
  }

  @Override
  public BaselineResult createBaseline() {
    final BaselineResult result = flyway.baseline();
    result.warnings.forEach(LOGGER::warn);
    return result;
  }

  @Override
  public String dumpSchema() throws IOException {
    return getDisclaimer() + new ExceptionWrappingDatabase(database).query(ctx -> ctx.meta()
        .filterSchemas(s -> SCHEMAS.contains(s.getName()))
        .ddl()
        .queryStream()
        .map(query -> query.toString() + ";")
        .filter(statement -> !statement.startsWith("create schema"))
        .collect(Collectors.joining("\n")));
  }

  protected String getDisclaimer() {
    return """
           // The content of the file is just to have a basic idea of the current state of the database and is not fully accurate.
           // It is also not used by any piece of code to generate anything.
           // It doesn't contain the enums created in the database and the default values might also be buggy.
           """ + '\n';
  }

  @VisibleForTesting
  public Database getDatabase() {
    return database;
  }

  @VisibleForTesting
  public Flyway getFlyway() {
    return flyway;
  }

}
