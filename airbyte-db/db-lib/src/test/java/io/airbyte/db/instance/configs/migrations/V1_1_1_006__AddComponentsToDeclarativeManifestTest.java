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
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V1_1_1_006__AddComponentsToDeclarativeManifestTest extends AbstractConfigsDatabaseTest {

  private static final String DECLARATIVE_MANIFEST = "declarative_manifest";
  private static final String COMPONENTS_FILE_CONTENT = "components_file_content";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_1_006__AddComponentsToDeclarativeManifest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V1_1_1_003__AddConnectionTagIndex();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testAddComponentsColumns() {
    final DSLContext context = getDslContext();
    final UUID actorDefinitionId = UUID.randomUUID();

    // Create a project before migration
    context.insertInto(DSL.table(DECLARATIVE_MANIFEST))
        .columns(
            DSL.field("actor_definition_id"),
            DSL.field("description"),
            DSL.field("manifest"),
            DSL.field("spec"),
            DSL.field("version"))
        .values(
            actorDefinitionId,
            "my epic connector",
            JSONB.valueOf("{}"),
            JSONB.valueOf("{}"),
            1)
        .execute();

    // Run migration
    V1_1_1_006__AddComponentsToDeclarativeManifest.runMigration(context);

    // Verify new column exists and is nullable
    Assertions.assertDoesNotThrow(() -> {
      context.update(DSL.table(DECLARATIVE_MANIFEST))
          .set(DSL.field(COMPONENTS_FILE_CONTENT), "test content")
          .where(DSL.field("actor_definition_id").eq(actorDefinitionId))
          .execute();
    });

    // Verify columns can be null
    Assertions.assertDoesNotThrow(() -> {
      context.update(DSL.table(DECLARATIVE_MANIFEST))
          .set(DSL.field(COMPONENTS_FILE_CONTENT), (String) null)
          .where(DSL.field("actor_definition_id").eq(actorDefinitionId))
          .execute();
    });
  }

}
