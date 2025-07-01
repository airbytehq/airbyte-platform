/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_006__AddComponentsToDeclarativeManifestTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_1_006__AddComponentsToDeclarativeManifest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V1_1_1_003__AddConnectionTagIndex()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testAddComponentsColumns() {
    val context = dslContext!!
    val actorDefinitionId = UUID.randomUUID()

    // Create a project before migration
    context
      .insertInto(DSL.table(DECLARATIVE_MANIFEST))
      .columns(
        DSL.field("actor_definition_id"),
        DSL.field("description"),
        DSL.field("manifest"),
        DSL.field("spec"),
        DSL.field("version"),
      ).values(
        actorDefinitionId,
        "my epic connector",
        JSONB.valueOf("{}"),
        JSONB.valueOf("{}"),
        1,
      ).execute()

    // Run migration
    V1_1_1_006__AddComponentsToDeclarativeManifest.runMigration(context)

    // Verify new column exists and is nullable
    Assertions.assertDoesNotThrow {
      context
        .update(DSL.table(DECLARATIVE_MANIFEST))
        .set(
          DSL.field(COMPONENTS_FILE_CONTENT),
          "test content",
        ).where(DSL.field("actor_definition_id").eq(actorDefinitionId))
        .execute()
    }

    // Verify columns can be null
    Assertions.assertDoesNotThrow {
      context
        .update(DSL.table(DECLARATIVE_MANIFEST))
        .set(
          DSL.field(COMPONENTS_FILE_CONTENT),
          null as String?,
        ).where(DSL.field("actor_definition_id").eq(actorDefinitionId))
        .execute()
    }
  }

  companion object {
    private const val DECLARATIVE_MANIFEST = "declarative_manifest"
    private const val COMPONENTS_FILE_CONTENT = "components_file_content"
  }
}
