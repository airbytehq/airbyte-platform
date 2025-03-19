/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert", "PMD.AvoidDuplicateLiterals"})
class V0_55_1_004__EnforceOrgsEverywhereTest extends AbstractConfigsDatabaseTest {

  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V055_1_003__EditRefreshTable", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_55_1_003__EditRefreshTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  /**
   * Test method that allows us to actually order things so the constraint doesn't interfere with our
   * first test's setup.
   */
  @Test
  void test() {
    testWithNoOrganization();
    testOrgIdNonNull();
  }

  private void testWithNoOrganization() {

    final DSLContext ctx = getDslContext();
    final UUID workspaceWithoutOrganization = UUID.randomUUID();
    final UUID workspaceWithOrganization = UUID.randomUUID();
    final UUID organizationId = UUID.randomUUID();

    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("notifications"))
        .values(
            workspaceWithoutOrganization,
            "name1",
            "default",
            true,
            JSONB.valueOf("[]"))
        .execute();

    ctx.insertInto(DSL.table("organization"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("email"))
        .values(
            organizationId,
            "org",
            "default")
        .execute();

    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("notifications"),
            DSL.field("organization_id"))
        .values(
            workspaceWithOrganization,
            "name1",
            "default",
            true,
            JSONB.valueOf("[]"),
            organizationId)
        .execute();

    V0_55_1_004__EnforceOrgsEverywhere.putAllWorkspacesWithoutOrgIntoDefaultOrg(ctx);

    final int workspaceIdsWithoutOrg = ctx.fetchCount(table("workspace").where(field("organization_id").isNull()));
    assertEquals(0, workspaceIdsWithoutOrg);

    final boolean workspaceInDefaultOrg = ctx.fetchExists(table("workspace").where(field("organization_id").eq(DEFAULT_ORGANIZATION_ID)));
    assertTrue(workspaceInDefaultOrg);

    final boolean workspaceWithOrgUnchanged = ctx.fetchExists(table("workspace")
        .where(field("id").eq(workspaceWithOrganization)
            .and(field("organization_id").eq(organizationId))));
    assertTrue(workspaceWithOrgUnchanged);
  }

  private void testOrgIdNonNull() {
    final DSLContext ctx = getDslContext();
    V0_55_1_004__EnforceOrgsEverywhere.setOrganizationIdNotNull(ctx);
    assertThrows(IntegrityConstraintViolationException.class, () -> insertWorkspaceWithNoOrganizationId(ctx));
  }

  private void insertWorkspaceWithNoOrganizationId(final DSLContext ctx) {
    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("notifications"))
        .values(
            UUID.randomUUID(),
            "name1",
            "default",
            true,
            JSONB.valueOf("[]"))
        .execute();
  }

}
