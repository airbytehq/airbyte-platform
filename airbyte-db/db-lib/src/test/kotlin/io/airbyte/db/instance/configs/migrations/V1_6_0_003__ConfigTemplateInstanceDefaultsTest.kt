/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
class V1_6_0_003__ConfigTemplateInstanceDefaultsTest : AbstractConfigsDatabaseTest() {
  companion object {
    // Tables
    private const val TABLE_CONFIG_TEMPLATE = "config_template"

    // Columns
    private const val COL_ID = "id"
    private const val COL_ACTOR_DEF_ID = "actor_definition_id"
    private const val COL_ACTOR_DEF_VERSION_ID = "actor_definition_version_id"
    private const val COL_ORG_ID = "organization_id"
    private const val COL_PARTIAL_DEFAULT_CONFIG = "partial_default_config"
    private const val USER_CONFIG_SPEC = "user_config_spec"

    private val actorDefId = UUID.randomUUID()
    private val orgId = UUID.randomUUID()
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_6_0_003__ConfigTemplateInstanceDefaultsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_6_0_002__AllowNullSecretConfigUser()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
    val ctx = dslContext!!
    dslContext!!.execute("ALTER TABLE config_template DROP CONSTRAINT IF EXISTS config_template_valid_reference_check")
    dslContext!!.execute("ALTER TABLE config_template DROP CONSTRAINT IF EXISTS config_template_actor_definition_version_id_unique")

    V1_6_0_003__ConfigTemplateInstanceDefaults.updateConfigTemplateTable(ctx)

    ctx.execute("ALTER TABLE config_template DROP CONSTRAINT IF EXISTS config_template_actor_definition_id_fkey")
    ctx.execute("ALTER TABLE config_template DROP CONSTRAINT IF EXISTS config_template_actor_definition_version_id_fkey")
    ctx.execute("ALTER TABLE config_template DROP CONSTRAINT IF EXISTS config_template_organization_id_fkey")
  }

  @Test
  fun `test can insert record with organization_id and actor_definition_id`() {
    val configId = UUID.randomUUID()

    dslContext!!
      .insertInto(DSL.table(TABLE_CONFIG_TEMPLATE))
      .columns(
        DSL.field(COL_ID),
        DSL.field(COL_ORG_ID),
        DSL.field(COL_ACTOR_DEF_ID),
        DSL.field(COL_PARTIAL_DEFAULT_CONFIG),
        DSL.field(USER_CONFIG_SPEC),
      ).values(
        configId,
        orgId,
        actorDefId,
        JSONB.jsonb("{}"),
        JSONB.jsonb("{}"),
      ).execute()

    val result =
      dslContext!!
        .selectFrom(DSL.table(TABLE_CONFIG_TEMPLATE))
        .where(DSL.field(COL_ID).eq(configId))
        .fetchOne()

    assertNotNull(result)
    assertEquals(orgId, result?.get(COL_ORG_ID, UUID::class.java))
    assertEquals(actorDefId, result?.get(COL_ACTOR_DEF_ID, UUID::class.java))
    assertNull(result?.get(COL_ACTOR_DEF_VERSION_ID, UUID::class.java))
  }

  @Test
  fun `test can insert record with actor_definition_version_id and no organization_id`() {
    val configId = UUID.randomUUID()
    val actorDefVersionId = UUID.randomUUID()

    dslContext!!
      .insertInto(DSL.table(TABLE_CONFIG_TEMPLATE))
      .columns(
        DSL.field(COL_ID),
        DSL.field(COL_ACTOR_DEF_VERSION_ID),
        DSL.field(COL_PARTIAL_DEFAULT_CONFIG),
        DSL.field(USER_CONFIG_SPEC),
      ).values(
        configId,
        actorDefVersionId,
        JSONB.jsonb("{}"),
        JSONB.jsonb("{}"),
      ).execute()

    val result =
      dslContext!!
        .selectFrom(DSL.table(TABLE_CONFIG_TEMPLATE))
        .where(DSL.field(COL_ID).eq(configId))
        .fetchOne()

    assertNotNull(result)
    assertNull(result?.get(COL_ORG_ID, UUID::class.java))
    assertNull(result?.get(COL_ACTOR_DEF_ID, UUID::class.java))
    assertEquals(actorDefVersionId, result?.get(COL_ACTOR_DEF_VERSION_ID, UUID::class.java))
  }

  @Test
  fun `test cannot insert record with both options`() {
    val configId = UUID.randomUUID()
    val actorDefVersionId = UUID.randomUUID()

    // Use assertThrows to test the constraint violation
    assertThrows(Exception::class.java) {
      dslContext!!
        .insertInto(DSL.table(TABLE_CONFIG_TEMPLATE))
        .columns(
          DSL.field(COL_ID),
          DSL.field(COL_ORG_ID),
          DSL.field(COL_ACTOR_DEF_ID),
          DSL.field(COL_ACTOR_DEF_VERSION_ID),
          DSL.field(COL_PARTIAL_DEFAULT_CONFIG),
          DSL.field(USER_CONFIG_SPEC),
        ).values(
          configId,
          orgId,
          actorDefId,
          actorDefVersionId,
          JSONB.jsonb("{}"),
          JSONB.jsonb("{}"),
        ).execute()
    }
  }

  @Test
  fun `test cannot insert record with none of the options`() {
    val configId = UUID.randomUUID()

    assertThrows(Exception::class.java) {
      // Try to insert without required fields
      dslContext!!
        .insertInto(DSL.table(TABLE_CONFIG_TEMPLATE))
        .columns(
          DSL.field(COL_ID),
          DSL.field(COL_PARTIAL_DEFAULT_CONFIG),
          DSL.field(USER_CONFIG_SPEC),
        ).values(
          configId,
          JSONB.jsonb("{}"),
          JSONB.jsonb("{}"),
        ).execute()
    }
  }
}
