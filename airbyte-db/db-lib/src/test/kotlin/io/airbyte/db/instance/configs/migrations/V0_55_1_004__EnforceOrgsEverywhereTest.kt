/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_55_1_004__EnforceOrgsEverywhere.Companion.putAllWorkspacesWithoutOrgIntoDefaultOrg
import io.airbyte.db.instance.configs.migrations.V0_55_1_004__EnforceOrgsEverywhere.Companion.setOrganizationIdNotNull
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_55_1_004__EnforceOrgsEverywhereTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V055_1_003__EditRefreshTable",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_55_1_003__EditRefreshTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  /**
   * Test method that allows us to actually order things so the constraint doesn't interfere with our
   * first test's setup.
   */
  @Test
  fun test() {
    testWithNoOrganization()
    testOrgIdNonNull()
  }

  private fun testWithNoOrganization() {
    val ctx = getDslContext()
    val workspaceWithoutOrganization = UUID.randomUUID()
    val workspaceWithOrganization = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field("notifications"),
      ).values(
        workspaceWithoutOrganization,
        "name1",
        "default",
        true,
        JSONB.valueOf("[]"),
      ).execute()

    ctx
      .insertInto(DSL.table("organization"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("email"),
      ).values(
        organizationId,
        "org",
        "default",
      ).execute()

    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field("notifications"),
        DSL.field("organization_id"),
      ).values(
        workspaceWithOrganization,
        "name1",
        "default",
        true,
        JSONB.valueOf("[]"),
        organizationId,
      ).execute()

    putAllWorkspacesWithoutOrgIntoDefaultOrg(ctx)

    val workspaceIdsWithoutOrg = ctx.fetchCount(DSL.table("workspace").where(DSL.field("organization_id").isNull()))
    Assertions.assertEquals(0, workspaceIdsWithoutOrg)

    val workspaceInDefaultOrg =
      ctx.fetchExists(
        DSL.table("workspace").where(
          DSL.field("organization_id").eq(
            DEFAULT_ORGANIZATION_ID,
          ),
        ),
      )
    Assertions.assertTrue(workspaceInDefaultOrg)

    val workspaceWithOrgUnchanged =
      ctx.fetchExists(
        DSL
          .table("workspace")
          .where(
            DSL
              .field("id")
              .eq(workspaceWithOrganization)
              .and(DSL.field("organization_id").eq(organizationId)),
          ),
      )
    Assertions.assertTrue(workspaceWithOrgUnchanged)
  }

  private fun testOrgIdNonNull() {
    val ctx = getDslContext()
    setOrganizationIdNotNull(ctx)
    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) { insertWorkspaceWithNoOrganizationId(ctx) }
  }

  private fun insertWorkspaceWithNoOrganizationId(ctx: DSLContext) {
    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field("notifications"),
      ).values(
        UUID.randomUUID(),
        "name1",
        "default",
        true,
        JSONB.valueOf("[]"),
      ).execute()
  }

  companion object {
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
