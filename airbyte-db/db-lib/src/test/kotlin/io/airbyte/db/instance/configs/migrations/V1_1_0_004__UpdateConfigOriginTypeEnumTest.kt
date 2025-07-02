/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_0_004__UpdateConfigOriginTypeEnumTest : AbstractConfigsDatabaseTest() {
  private var configsDbMigrator: ConfigsDatabaseMigrator? = null

  @BeforeEach
  fun setUp() {
    val flyway =
      create(
        dataSource,
        "V1_1_0_004__UpdateConfigOriginTypeEnumTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    // Initialize the database with migrations up to, but not including, our target migration
    val previousMigration: BaseJavaMigration = V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator!!, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testMigration() {
    val ctx = dslContext!!
    // Insert a record with RELEASE_CANDIDATE before migration
    val releaseCandidateId = insertRecordWithOriginReleaseCandidate(ctx)
    Assertions.assertNotNull(releaseCandidateId)

    V1_1_0_004__UpdateConfigOriginTypeEnum.runMigration(ctx)

    verifyAllRecordsUpdated(ctx)
    Assertions.assertThrows(
      Exception::class.java,
    ) { insertRecordWithOriginReleaseCandidate(ctx) }
    val connectorRolloutId = insertRecordWithOriginConnectorRollout(ctx)
    Assertions.assertNotNull(connectorRolloutId)
  }

  private fun insertRecordWithOriginReleaseCandidate(ctx: DSLContext): UUID {
    val configId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(SCOPED_CONFIGURATION_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("key"),
        DSL.field("resource_type"),
        DSL.field("resource_id"),
        DSL.field("scope_type"),
        DSL.field("scope_id"),
        DSL.field("value"),
        DSL.field("description"),
        DSL.field("reference_url"),
        DSL.field("origin_type"),
        DSL.field("origin"),
        DSL.field("expires_at"),
      ).values(
        configId,
        "testKey",
        DSL.field("?::config_resource_type", "actor_definition"),
        UUID.randomUUID(),
        DSL.field("?::config_scope_type", "workspace"),
        UUID.randomUUID(),
        "testValue",
        "testDescription",
        "testUrl",
        DSL.field("?::config_origin_type", "release_candidate"),
        "testOrigin",
        OffsetDateTime.now(),
      ).execute()
    return configId
  }

  private fun insertRecordWithOriginConnectorRollout(ctx: DSLContext): UUID {
    val configId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(SCOPED_CONFIGURATION_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("key"),
        DSL.field("resource_type"),
        DSL.field("resource_id"),
        DSL.field("scope_type"),
        DSL.field("scope_id"),
        DSL.field("value"),
        DSL.field("description"),
        DSL.field("reference_url"),
        DSL.field("origin_type"),
        DSL.field("origin"),
        DSL.field("expires_at"),
      ).values(
        configId,
        "testKey",
        DSL.field("?::config_resource_type", "actor_definition"),
        UUID.randomUUID(),
        DSL.field("?::config_scope_type", "workspace"),
        UUID.randomUUID(),
        "testValue",
        "testDescription",
        "testUrl",
        DSL.field("?::config_origin_type", "connector_rollout"),
        "testOrigin",
        OffsetDateTime.now(),
      ).execute()
    return configId
  }

  private fun verifyAllRecordsUpdated(ctx: DSLContext) {
    var count =
      ctx
        .selectCount()
        .from(SCOPED_CONFIGURATION_TABLE)
        .where(
          DSL
            .field(ORIGIN_TYPE_COLUMN)
            .cast(
              String::class.java,
            ).eq(RELEASE_CANDIDATE),
        ).fetchOne(0, Int::class.javaPrimitiveType)!!
    Assertions.assertEquals(0, count, "There should be no RELEASE_CANDIDATE records after migration")

    count =
      ctx
        .selectCount()
        .from(SCOPED_CONFIGURATION_TABLE)
        .where(
          DSL
            .field(ORIGIN_TYPE_COLUMN)
            .cast(
              String::class.java,
            ).eq(CONNECTOR_ROLLOUT),
        ).fetchOne(0, Int::class.javaPrimitiveType)!!
    Assertions.assertTrue(count > 0, "There should be at least one CONNECTOR_ROLLOUT record after migration")
  }

  companion object {
    private const val SCOPED_CONFIGURATION_TABLE = "scoped_configuration"
    private const val ORIGIN_TYPE_COLUMN = "origin_type"
    private const val RELEASE_CANDIDATE = "release_candidate"
    private const val CONNECTOR_ROLLOUT = "connector_rollout"
  }
}
