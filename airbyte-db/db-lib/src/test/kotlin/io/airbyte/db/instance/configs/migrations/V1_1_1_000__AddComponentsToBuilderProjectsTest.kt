/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_000__AddComponentsToBuilderProjectsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_1_000__AddComponentsToBuilderProjectsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V1_1_0_010__CreateTagTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testAddComponentsColumns() {
    val context = getDslContext()
    val projectId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    // Create a project before migration
    context
      .insertInto(DSL.table(CONNECTOR_BUILDER_PROJECT))
      .columns(
        DSL.field("id"),
        DSL.field(WORKSPACE_ID),
        DSL.field(NAME),
      ).values(
        projectId,
        workspaceId,
        "test project",
      ).execute()

    // Run migration
    V1_1_1_000__AddComponentsToBuilderProjects.runMigration(context)

    // Verify new columns exist and are nullable
    Assertions.assertDoesNotThrow {
      context
        .update(DSL.table(CONNECTOR_BUILDER_PROJECT))
        .set(
          DSL.field(COMPONENTS_FILE_CONTENT),
          "test content",
        ).where(DSL.field("id").eq(projectId))
        .execute()
    }

    // Verify columns can be null
    Assertions.assertDoesNotThrow {
      context
        .update(DSL.table(CONNECTOR_BUILDER_PROJECT))
        .set(
          DSL.field(COMPONENTS_FILE_CONTENT),
          null as String?,
        ).where(DSL.field("id").eq(projectId))
        .execute()
    }
  }

  companion object {
    private const val CONNECTOR_BUILDER_PROJECT = "connector_builder_project"
    private const val WORKSPACE_ID = "workspace_id"
    private const val NAME = "name"
    private const val COMPONENTS_FILE_CONTENT = "components_file_content"
  }
}
