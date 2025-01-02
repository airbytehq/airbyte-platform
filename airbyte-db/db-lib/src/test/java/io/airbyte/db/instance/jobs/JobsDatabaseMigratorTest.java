/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.DatabaseMigrator;
import io.airbyte.db.instance.development.MigrationDevHelper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class JobsDatabaseMigratorTest extends AbstractJobsDatabaseTest {

  @Test
  void dumpSchema() throws IOException {
    final File schemaDumpFile = File.createTempFile("jobs-schema-dump", "txt");
    schemaDumpFile.deleteOnExit();
    final Flyway flyway = FlywayFactory.create(getDataSource(), getClass().getSimpleName(), JobsDatabaseMigrator.DB_IDENTIFIER,
        JobsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final DatabaseMigrator migrator = new JobsDatabaseMigrator(database, flyway);
    migrator.migrate();
    final String schema = migrator.dumpSchema();
    MigrationDevHelper.dumpSchema(schema, schemaDumpFile.getAbsolutePath(), false);
    final String dumpedSchema = FileUtils.readFileToString(schemaDumpFile, StandardCharsets.UTF_8);

    assertTrue(schemaDumpFile.exists());
    assertEquals(schema.trim(), dumpedSchema.trim());
  }

}
