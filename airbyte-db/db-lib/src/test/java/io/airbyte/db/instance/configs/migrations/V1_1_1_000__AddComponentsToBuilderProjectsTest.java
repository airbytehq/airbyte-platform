/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V1_1_1_000__AddComponentsToBuilderProjectsTest extends AbstractConfigsDatabaseTest {

  private static final String CONNECTOR_BUILDER_PROJECT = "connector_builder_project";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final String NAME = "name";
  private static final String COMPONENTS_FILE_CONTENT = "components_file_content";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_1_000__AddComponentsToBuilderProjectsTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V1_1_0_010__CreateTagTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testAddComponentsColumns() {
    final DSLContext context = getDslContext();
    final UUID projectId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();

    // Create a project before migration
    context.insertInto(DSL.table(CONNECTOR_BUILDER_PROJECT))
        .columns(
            DSL.field("id"),
            DSL.field(WORKSPACE_ID),
            DSL.field(NAME))
        .values(
            projectId,
            workspaceId,
            "test project")
        .execute();

    // Run migration
    V1_1_1_000__AddComponentsToBuilderProjects.runMigration(context);

    // Verify new columns exist and are nullable
    Assertions.assertDoesNotThrow(() -> {
      context.update(DSL.table(CONNECTOR_BUILDER_PROJECT))
          .set(DSL.field(COMPONENTS_FILE_CONTENT), "test content")
          .where(DSL.field("id").eq(projectId))
          .execute();
    });

    // Verify columns can be null
    Assertions.assertDoesNotThrow(() -> {
      context.update(DSL.table(CONNECTOR_BUILDER_PROJECT))
          .set(DSL.field(COMPONENTS_FILE_CONTENT), (String) null)
          .where(DSL.field("id").eq(projectId))
          .execute();
    });
  }

}
