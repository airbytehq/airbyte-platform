/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V1_1_0_010__CreateTagTable.Companion.createTagTable
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.exception.DataAccessException
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_0_010__CreateTagTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_0_010__CreateTagTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V1_1_0_009__AddPausedReasonToConnectorRollout()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    val context = getDslContext()
    createTagTable(context)

    // Create a workspace to add the tags to
    context
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field("organization_id"),
      ).values(
        workspaceId,
        "default",
        "default",
        true,
        OrganizationPersistence.DEFAULT_ORGANIZATION_ID,
      ).execute()
  }

  @AfterEach
  fun teardown() {
    // Fully tear down db after each test
    val dslContext = getDslContext()
    dslContext.dropSchemaIfExists("public").cascade().execute()
    dslContext.createSchema("public").execute()
    dslContext.setSchema("public").execute()
  }

  @Test
  fun testCanInsertTag() {
    val context = getDslContext()
    val tagId = UUID.randomUUID()

    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table(TAG))
        .columns(
          DSL.field(ID),
          DSL.field(NAME),
          DSL.field(COLOR),
          DSL.field(WORKSPACE_ID),
        ).values(
          tagId,
          "Some tag",
          "000000",
          workspaceId,
        ).execute()
    }
  }

  @Test
  fun testDuplicateTestNameConstraint() {
    val context = getDslContext()
    val tagName = "Some tag"

    val firstTagId = UUID.randomUUID()
    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table(TAG))
        .columns(
          DSL.field(ID),
          DSL.field(NAME),
          DSL.field(COLOR),
          DSL.field(WORKSPACE_ID),
        ).values(
          firstTagId,
          tagName,
          "000000",
          workspaceId,
        ).execute()
    }

    val secondTagId = UUID.randomUUID()
    val e: Exception =
      Assertions.assertThrows(
        IntegrityConstraintViolationException::class.java,
      ) {
        context
          .insertInto(DSL.table(TAG))
          .columns(
            DSL.field(ID),
            DSL.field(NAME),
            DSL.field(COLOR),
            DSL.field(WORKSPACE_ID),
          ).values(
            secondTagId,
            tagName,
            "000000",
            workspaceId,
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("duplicate key value violates unique constraint \"tag_name_workspace_id_key\""))
  }

  @Test
  fun testValidHexConstraint() {
    val context = getDslContext()
    val tagId = UUID.randomUUID()

    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(DSL.table(TAG))
          .columns(
            DSL.field(ID),
            DSL.field(NAME),
            DSL.field(COLOR),
            DSL.field(WORKSPACE_ID),
          ).values(
            tagId,
            "Some tag",
            "NOTHEX",
            workspaceId,
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("new row for relation \"tag\" violates check constraint \"valid_hex_color\""))
  }

  companion object {
    private const val TAG = "tag"
    private const val ID = "id"
    private const val WORKSPACE_ID = "workspace_id"
    private const val NAME = "name"
    private const val COLOR = "color"
    private val workspaceId: UUID = UUID.randomUUID()
  }
}
