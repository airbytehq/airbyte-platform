/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_50_19_001__CreateDefaultOrganizationAndUserTest extends AbstractConfigsDatabaseTest {

  private static final UUID EXPECTED_DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final UUID EXPECTED_DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
  private static final String USER_TABLE = "\"user\"";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_19_001__CreateDefaultOrganizationAndUser", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_16_002__RemoveInvalidSourceStripeCatalog();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @AfterEach
  void afterEach() {
    // Making sure we reset between tests
    dslContext.dropSchemaIfExists("public").cascade().execute();
    dslContext.createSchema("public").execute();
    dslContext.setSchema("public").execute();
  }

  @Test
  void testMigrationBlankDatabase() {
    final DSLContext ctx = getDslContext();

    // this test is specifically ensuring that lack of a workspace record does not cause an error
    // while creating the default user and organization.
    assertEquals(0, ctx.fetchCount(DSL.table("workspace")));

    V0_50_19_001__CreateDefaultOrganizationAndUser.createDefaultUserAndOrganization(ctx);

    // verify that a User record was written to the database
    final var userRecord = ctx.selectFrom(DSL.table(USER_TABLE))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetchOne();
    assertNotNull(userRecord);
    assertEquals("", userRecord.get(DSL.field("email", String.class)));
    assertEquals("Default User", userRecord.get(DSL.field("name", String.class)));
    assertNull(userRecord.get(DSL.field("default_workspace_id", UUID.class)));

    // verify that an Organization record was written to the database
    final var organizationRecord = ctx.selectFrom(DSL.table("organization"))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_ORGANIZATION_ID))
        .fetchOne();
    assertNotNull(organizationRecord);
    assertEquals("", organizationRecord.get(DSL.field("email", String.class)));
    assertEquals("Default Organization", organizationRecord.get(DSL.field("name", String.class)));
    assertEquals(EXPECTED_DEFAULT_USER_ID, organizationRecord.get(DSL.field("user_id", UUID.class)));

    // verify that a permission record was written to add the User to the Organization
    final var userPermissionRecord = ctx.selectFrom(DSL.table("permission"))
        .where(DSL.field("user_id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetch()
        .get(0);
    assertNotNull(userPermissionRecord);
    assertEquals(EXPECTED_DEFAULT_ORGANIZATION_ID, userPermissionRecord.get(DSL.field("organization_id", SQLDataType.UUID)));
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testMigrationExistingWorkspace(final Boolean initialSetupComplete) {
    final DSLContext ctx = getDslContext();

    // 1. Setup initial workspace records in database for pre-migration state

    // this workspace should not be assigned during the migration despite being first, because it
    // is always tombstoned.
    insertWorkspace(ctx, "tombstoned@airbyte.com", true, true);

    if (initialSetupComplete) {
      // for this test case, insert an earlier workspace that has initialSetup: false to make sure it
      // isn't chosen in favor of the expected workspace with initialSetup: true
      insertWorkspace(ctx, "", false, false);
    }

    final String workspaceEmail = initialSetupComplete ? "expected@airbyte.com" : null;
    final UUID expectedWorkspaceId = insertWorkspace(ctx, workspaceEmail, initialSetupComplete, false);

    // this last workspace should never be used for anything because it comes later than the expected
    // workspace
    insertWorkspace(ctx, null, false, false);

    // 2. Set up expected User record result based on the test case
    final String expectedUserEmail = initialSetupComplete ? workspaceEmail : "";
    final String expectedUserName = "Default User";

    // 3. Set up expected Organization record result based on the test case
    // initial organization should use the workspace email if its initial setup was complete,
    // or blank if not.
    final String expectedOrganizationEmail = initialSetupComplete ? workspaceEmail : "";
    final String expectedOrganizationName = "Default Organization";

    // 4. run the migration
    V0_50_19_001__CreateDefaultOrganizationAndUser.createDefaultUserAndOrganization(ctx);

    // 5. verify that the expected User records now exist in the database
    final var userRecord = ctx.selectFrom(DSL.table(USER_TABLE))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetchOne();
    assertNotNull(userRecord);
    assertEquals(expectedUserEmail, userRecord.get(DSL.field("email", String.class)));
    assertEquals(expectedUserName, userRecord.get(DSL.field("name", String.class)));
    assertEquals(expectedWorkspaceId, userRecord.get(DSL.field("default_workspace_id", UUID.class)));

    // 6. verify that the expected Organization records now exist in the database
    final var organizationRecord = ctx.selectFrom(DSL.table("organization"))
        .where(DSL.field("id").eq(EXPECTED_DEFAULT_ORGANIZATION_ID))
        .fetchOne();
    assertNotNull(organizationRecord);
    assertEquals(expectedOrganizationEmail, organizationRecord.get(DSL.field("email", String.class)));
    assertEquals(expectedOrganizationName, organizationRecord.get(DSL.field("name", String.class)));
    assertEquals(EXPECTED_DEFAULT_USER_ID, organizationRecord.get(DSL.field("user_id", UUID.class)));

    // 7. verify that the default Workspace record was updated to belong to the default Organization
    final var workspaceRecord = ctx.selectFrom(DSL.table("workspace"))
        .where(DSL.field("id").eq(expectedWorkspaceId))
        .fetchOne();
    assertNotNull(workspaceRecord);
    assertEquals(EXPECTED_DEFAULT_ORGANIZATION_ID, workspaceRecord.get(DSL.field("organization_id", UUID.class)));

    // 8. verify that a permission record was written to add the User to the Organization
    final var userPermissionRecord = ctx.selectFrom(DSL.table("permission"))
        .where(DSL.field("user_id").eq(EXPECTED_DEFAULT_USER_ID))
        .fetch()
        .get(0);
    assertNotNull(userPermissionRecord);
    assertEquals(EXPECTED_DEFAULT_ORGANIZATION_ID, userPermissionRecord.get(DSL.field("organization_id", SQLDataType.UUID)));
  }

  private UUID insertWorkspace(final DSLContext ctx, final String email, final Boolean initialSetupComplete, final Boolean tombstone) {
    final UUID workspaceId = UUID.randomUUID();

    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("email"),
            DSL.field("initial_setup_complete"),
            DSL.field("tombstone"),
            DSL.field("name"),
            DSL.field("slug"))
        .values(
            workspaceId,
            email,
            initialSetupComplete,
            tombstone,
            "workspace",
            "slug")
        .execute();
    return workspaceId;
  }

}
