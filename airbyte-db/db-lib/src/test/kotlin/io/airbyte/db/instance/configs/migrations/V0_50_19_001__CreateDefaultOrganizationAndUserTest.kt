/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_19_001__CreateDefaultOrganizationAndUser.Companion.createDefaultUserAndOrganization
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_19_001__CreateDefaultOrganizationAndUserTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_19_001__CreateDefaultOrganizationAndUser",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_16_002__RemoveInvalidSourceStripeCatalog()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @AfterEach
  fun afterEach() {
    // Making sure we reset between tests
    dslContext.dropSchemaIfExists("public").cascade().execute()
    dslContext.createSchema("public").execute()
    dslContext.setSchema("public").execute()
  }

  @Test
  fun testMigrationBlankDatabase() {
    val ctx = getDslContext()

    // this test is specifically ensuring that lack of a workspace record does not cause an error
    // while creating the default user and organization.
    Assertions.assertEquals(0, ctx.fetchCount(DSL.table("workspace")))

    createDefaultUserAndOrganization(ctx)

    // verify that a User record was written to the database
    val userRecord =
      ctx
        .selectFrom(DSL.table(USER_TABLE))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetchOne()
    Assertions.assertNotNull(userRecord)
    Assertions.assertEquals(
      "",
      userRecord!!.get(
        DSL.field(
          "email",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      "Default User",
      userRecord.get(
        DSL.field(
          "name",
          String::class.java,
        ),
      ),
    )
    Assertions.assertNull(
      userRecord.get(
        DSL.field(
          "default_workspace_id",
          UUID::class.java,
        ),
      ),
    )

    // verify that an Organization record was written to the database
    val organizationRecord =
      ctx
        .selectFrom(DSL.table("organization"))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_ORGANIZATION_ID))
        .fetchOne()
    Assertions.assertNotNull(organizationRecord)
    Assertions.assertEquals(
      "",
      organizationRecord!!.get(
        DSL.field(
          "email",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      "Default Organization",
      organizationRecord.get(
        DSL.field(
          "name",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      EXPECTED_DEFAULT_USER_ID,
      organizationRecord.get(
        DSL.field(
          "user_id",
          UUID::class.java,
        ),
      ),
    )

    // verify that a permission record was written to add the User to the Organization
    val userPermissionRecord =
      ctx
        .selectFrom(DSL.table("permission"))
        .where(DSL.field("user_id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetch()[0]
    Assertions.assertNotNull(userPermissionRecord)
    Assertions.assertEquals(
      EXPECTED_DEFAULT_ORGANIZATION_ID,
      userPermissionRecord.get(DSL.field("organization_id", SQLDataType.UUID)),
    )
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  fun testMigrationExistingWorkspace(initialSetupComplete: Boolean) {
    val ctx = getDslContext()

    // 1. Setup initial workspace records in database for pre-migration state

    // this workspace should not be assigned during the migration despite being first, because it
    // is always tombstoned.
    insertWorkspace(ctx, "tombstoned@airbyte.com", true, true)

    if (initialSetupComplete) {
      // for this test case, insert an earlier workspace that has initialSetup: false to make sure it
      // isn't chosen in favor of the expected workspace with initialSetup: true
      insertWorkspace(ctx, "", false, false)
    }

    val workspaceEmail = if (initialSetupComplete) "expected@airbyte.com" else null
    val expectedWorkspaceId = insertWorkspace(ctx, workspaceEmail, initialSetupComplete, false)

    // this last workspace should never be used for anything because it comes later than the expected
    // workspace
    insertWorkspace(ctx, null, false, false)

    // 2. Set up expected User record result based on the test case
    val expectedUserEmail = if (initialSetupComplete) workspaceEmail!! else ""
    val expectedUserName = "Default User"

    // 3. Set up expected Organization record result based on the test case
    // initial organization should use the workspace email if its initial setup was complete,
    // or blank if not.
    val expectedOrganizationEmail = if (initialSetupComplete) workspaceEmail!! else ""
    val expectedOrganizationName = "Default Organization"

    // 4. run the migration
    createDefaultUserAndOrganization(ctx)

    // 5. verify that the expected User records now exist in the database
    val userRecord =
      ctx
        .selectFrom(DSL.table(USER_TABLE))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetchOne()
    Assertions.assertNotNull(userRecord)
    Assertions.assertEquals(
      expectedUserEmail,
      userRecord!!.get(
        DSL.field(
          "email",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      expectedUserName,
      userRecord.get(
        DSL.field(
          "name",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      expectedWorkspaceId,
      userRecord.get(
        DSL.field(
          "default_workspace_id",
          UUID::class.java,
        ),
      ),
    )

    // 6. verify that the expected Organization records now exist in the database
    val organizationRecord =
      ctx
        .selectFrom(DSL.table("organization"))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_ORGANIZATION_ID))
        .fetchOne()
    Assertions.assertNotNull(organizationRecord)
    Assertions.assertEquals(
      expectedOrganizationEmail,
      organizationRecord!!.get(
        DSL.field(
          "email",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      expectedOrganizationName,
      organizationRecord.get(
        DSL.field(
          "name",
          String::class.java,
        ),
      ),
    )
    Assertions.assertEquals(
      EXPECTED_DEFAULT_USER_ID,
      organizationRecord.get(
        DSL.field(
          "user_id",
          UUID::class.java,
        ),
      ),
    )

    // 7. verify that the default Workspace record was updated to belong to the default Organization
    val workspaceRecord =
      ctx
        .selectFrom(DSL.table("workspace"))
        .where(DSL.field("id").eq(expectedWorkspaceId))
        .fetchOne()
    Assertions.assertNotNull(workspaceRecord)
    Assertions.assertEquals(
      EXPECTED_DEFAULT_ORGANIZATION_ID,
      workspaceRecord!!.get(
        DSL.field(
          "organization_id",
          UUID::class.java,
        ),
      ),
    )

    // 8. verify that a permission record was written to add the User to the Organization
    val userPermissionRecord =
      ctx
        .selectFrom(DSL.table("permission"))
        .where(DSL.field("user_id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetch()[0]
    Assertions.assertNotNull(userPermissionRecord)
    Assertions.assertEquals(
      EXPECTED_DEFAULT_ORGANIZATION_ID,
      userPermissionRecord.get(DSL.field("organization_id", SQLDataType.UUID)),
    )
  }

  private fun insertWorkspace(
    ctx: DSLContext,
    email: String?,
    initialSetupComplete: Boolean,
    tombstone: Boolean,
  ): UUID {
    val workspaceId = UUID.randomUUID()

    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("email"),
        DSL.field("initial_setup_complete"),
        DSL.field("tombstone"),
        DSL.field("name"),
        DSL.field("slug"),
      ).values(
        workspaceId,
        email,
        initialSetupComplete,
        tombstone,
        "workspace",
        "slug",
      ).execute()
    return workspaceId
  }

  companion object {
    private val EXPECTED_DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val EXPECTED_DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
    private const val USER_TABLE = "\"user\""
  }
}
