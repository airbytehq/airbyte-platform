/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.select;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.configs.migrations.V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTableTest extends AbstractConfigsDatabaseTest {

  private static final String ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE = "actor_definition_workspace_grant";
  private static final String ACTOR_DEFINITION_ID = "actor_definition_id";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final String SCOPE_ID = "scope_id";
  private static final String SCOPE_TYPE = "scope_type";
  public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private DevDatabaseMigrator devConfigsDbMigrator;

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_5_004__AddActorDefinitionBreakingChangeTable();
    devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
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
  void testSimpleMigration() {
    final DSLContext context = getDslContext();
    final UUID actorDefinitionId = UUID.randomUUID();
    final UUID workspaceId1 = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();

    addDefaultOrganization(context);

    addWorkspace(context, workspaceId1);
    addWorkspace(context, workspaceId2);

    addActorDefinition(context, actorDefinitionId);

    // Adding initial actor_definition_workspace_grant
    context.insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
        .columns(
            DSL.field(ACTOR_DEFINITION_ID),
            DSL.field(WORKSPACE_ID))
        .values(
            actorDefinitionId,
            workspaceId1)
        .execute();

    context.insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
        .columns(
            DSL.field(ACTOR_DEFINITION_ID),
            DSL.field(WORKSPACE_ID))
        .values(
            actorDefinitionId,
            workspaceId2)
        .execute();

    // Applying the migration
    devConfigsDbMigrator.migrate();

    Assertions.assertTrue(scopeColumnsExists(context), "column is missing");
    Assertions.assertTrue(scopeColumnsMatchWorkspaceId(context, actorDefinitionId, workspaceId1), "workspace id 1 doesn't match");
    Assertions.assertTrue(scopeColumnsMatchWorkspaceId(context, actorDefinitionId, workspaceId2), "workspace id 2 doesn't match");
  }

  private static void addDefaultOrganization(final DSLContext context) {
    final Field<UUID> idColumn = DSL.field("id", SQLDataType.UUID);
    final Field<String> emailColumn = DSL.field("email", SQLDataType.VARCHAR(256));
    final Field<String> nameColumn = DSL.field("name", SQLDataType.VARCHAR(256));
    final Field<UUID> userIdColumn = DSL.field("user_id", SQLDataType.UUID);

    context.insertInto(DSL.table("organization"))
        .columns(idColumn, emailColumn, nameColumn, userIdColumn)
        .values(DEFAULT_UUID, "test@test.com", "Default Organization", DEFAULT_UUID)
        .execute();
  }

  protected static boolean scopeColumnsExists(final DSLContext ctx) {
    return ctx.fetchExists(select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
            .and(DSL.field("column_name").eq(SCOPE_ID))))
        &&
        ctx.fetchExists(select()
            .from("information_schema.columns")
            .where(DSL.field("table_name").eq(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
                .and(DSL.field("column_name").eq(SCOPE_TYPE))));
  }

  protected static boolean scopeColumnsMatchWorkspaceId(final DSLContext ctx, final UUID actorDefinitionId, final UUID workspaceId) {
    final Record record = ctx.fetchOne(select()
        .from(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
        .where(DSL.field(ACTOR_DEFINITION_ID).eq(actorDefinitionId))
        .and(DSL.field(WORKSPACE_ID).eq(workspaceId)));

    assert record != null;
    return record.get(SCOPE_ID).equals(workspaceId) && record.get(SCOPE_TYPE).toString().equals(ScopeTypeEnum.workspace.getLiteral());
  }

  private static void addWorkspace(final DSLContext ctx, final UUID workspaceId) {
    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("organization_id"))
        .values(
            workspaceId,
            "base workspace",
            "base_workspace",
            true,
            DEFAULT_UUID)
        .execute();
  }

  private static void addActorDefinition(final DSLContext ctx, final UUID actorDefinitionId) {
    ctx.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            actorDefinitionId,
            "name",
            ActorType.source)
        .execute();
  }

  @Test
  void testUniquenessConstraint() {
    devConfigsDbMigrator.migrate();
    final UUID actorDefinitionId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID scopeId = UUID.randomUUID();

    final DSLContext context = getDslContext();

    // We retroactively made orgs required so applying the default org/user migration so we can use the
    // default org to allow this test to pass
    V0_50_19_001__CreateDefaultOrganizationAndUser.createDefaultUserAndOrganization(context);

    addWorkspace(context, workspaceId);
    addActorDefinition(context, actorDefinitionId);

    context.insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
        .columns(
            DSL.field(ACTOR_DEFINITION_ID),
            DSL.field(SCOPE_ID),
            DSL.field(SCOPE_TYPE))
        .values(actorDefinitionId, scopeId, ScopeTypeEnum.workspace)
        .execute();

    context.insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
        .columns(
            DSL.field(ACTOR_DEFINITION_ID),
            DSL.field(SCOPE_ID),
            DSL.field(SCOPE_TYPE))
        .values(actorDefinitionId, scopeId, ScopeTypeEnum.organization)
        .execute();

    Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
          .columns(
              DSL.field(ACTOR_DEFINITION_ID),
              DSL.field(SCOPE_ID),
              DSL.field(SCOPE_TYPE))
          .values(actorDefinitionId, scopeId, ScopeTypeEnum.workspace)
          .execute();
    });
  }

}
