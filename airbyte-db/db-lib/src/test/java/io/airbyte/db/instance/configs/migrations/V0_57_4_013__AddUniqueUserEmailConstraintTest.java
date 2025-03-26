/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType;
import io.airbyte.db.instance.configs.migrations.V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.InvitationStatus;
import io.airbyte.db.instance.configs.migrations.V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@TestMethodOrder(OrderAnnotation.class)
class V0_57_4_013__AddUniqueUserEmailConstraintTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> USER_TABLE = DSL.table("\"user\"");
  private static final Field<String> EMAIL = DSL.field("email", SQLDataType.VARCHAR);
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<OffsetDateTime> CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<UUID> INVITER_USER_ID = DSL.field("inviter_user_id", SQLDataType.UUID);
  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private String email;

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_57_4_013__AddUniqueUserEmailConstraintTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    email = UUID.randomUUID() + "@airbyte.io";

    // Remove constraint preventing timeline event creation
    dropTimelineConnectionFK(getDslContext());
  }

  @Test
  @Order(10)
  void testSSOUserMigration() {
    final DSLContext ctx = getDslContext();

    final UUID ssoUserId = UUID.randomUUID();
    createUser(ctx, ssoUserId, email, OffsetDateTime.now());
    makeUserSSO(ctx, ssoUserId);

    final UUID userId = UUID.randomUUID();
    createUser(ctx, userId, email, OffsetDateTime.now().minusDays(1));
    createInvitationFromUser(ctx, userId);
    createTimelineEventByUser(ctx, userId);

    V0_57_4_013__AddUniqueUserEmailConstraint.deleteDuplicateUsers(ctx);

    assertUserReplaced(ctx, userId, ssoUserId);
    assertNoDuplicateEmails(ctx);
  }

  @Test
  @Order(10)
  void testMoreThanOneSSOUserKeepsOldest() {
    final DSLContext ctx = getDslContext();

    final UUID ssoUserId = UUID.randomUUID();
    createUser(ctx, ssoUserId, email, OffsetDateTime.now());
    createInvitationFromUser(ctx, ssoUserId);
    createTimelineEventByUser(ctx, ssoUserId);
    makeUserSSO(ctx, ssoUserId);

    final UUID ssoUserId2 = UUID.randomUUID();
    createUser(ctx, ssoUserId2, email, OffsetDateTime.now().minusDays(1));
    makeUserSSO(ctx, ssoUserId2);

    V0_57_4_013__AddUniqueUserEmailConstraint.deleteDuplicateUsers(ctx);

    assertUserReplaced(ctx, ssoUserId, ssoUserId2);
    assertNoDuplicateEmails(ctx);
  }

  @Test
  @Order(10)
  void testNonSSOKeepOldestUser() {
    final DSLContext ctx = getDslContext();

    final UUID userId = UUID.randomUUID();
    createUser(ctx, userId, email, OffsetDateTime.now());
    createInvitationFromUser(ctx, userId);
    createTimelineEventByUser(ctx, userId);

    final UUID userId2 = UUID.randomUUID();
    createUser(ctx, userId2, email, OffsetDateTime.now().minusDays(1));

    V0_57_4_013__AddUniqueUserEmailConstraint.deleteDuplicateUsers(ctx);

    assertUserReplaced(ctx, userId, userId2);
    assertNoDuplicateEmails(ctx);
  }

  @Test
  @Order(10)
  void testUnsetDefaultUserEmail() {
    final DSLContext ctx = getDslContext();

    ctx.update(USER_TABLE)
        .set(EMAIL, email)
        .where(ID.eq(DEFAULT_USER_ID))
        .execute();

    final UUID userId2 = UUID.randomUUID();
    createUser(ctx, userId2, email, OffsetDateTime.now());
    createInvitationFromUser(ctx, userId2);
    createTimelineEventByUser(ctx, userId2);

    V0_57_4_013__AddUniqueUserEmailConstraint.deleteDuplicateUsers(ctx);

    assertUserExists(ctx, DEFAULT_USER_ID);
    assertUserExists(ctx, userId2);
    assertNoDuplicateEmails(ctx);

    final var defaultUserEmail = ctx.select(EMAIL)
        .from(USER_TABLE)
        .where(ID.eq(DEFAULT_USER_ID))
        .fetchOptional();
    Assertions.assertTrue(defaultUserEmail.isPresent());
    Assertions.assertEquals("", defaultUserEmail.get().value1());
  }

  @Test
  @Order(100)
  void testUniqueConstraint() {
    final DSLContext ctx = getDslContext();
    V0_57_4_013__AddUniqueUserEmailConstraint.addUniqueUserEmailConstraint(ctx);

    final String email = "bob@airbyte.io";

    createUser(ctx, UUID.randomUUID(), email, OffsetDateTime.now());
    Assertions.assertThrows(Exception.class, () -> createUser(ctx, UUID.randomUUID(), email, OffsetDateTime.now()));

    final String alternateCasingEmail = "BOB@airbyte.io";
    Assertions.assertThrows(Exception.class, () -> createUser(ctx, UUID.randomUUID(), alternateCasingEmail, OffsetDateTime.now()));

    createUser(ctx, UUID.randomUUID(), "anotherone@airbyte.io", OffsetDateTime.now());

    assertNoDuplicateEmails(ctx);
  }

  private static void createUser(final DSLContext ctx, final UUID id, final String email, final OffsetDateTime createdAt) {
    ctx.insertInto(USER_TABLE, ID, EMAIL, DSL.field("name"), CREATED_AT)
        .values(id, email, "Name", createdAt)
        .execute();

  }

  private void assertUserReplaced(final DSLContext ctx, final UUID deletedUserId, final UUID replacementUserId) {
    assertUserExists(ctx, replacementUserId);
    Assertions.assertEquals(0, ctx.fetchCount(ctx.selectFrom(USER_TABLE).where(ID.eq(deletedUserId))));
    Assertions.assertEquals(1, ctx.fetchCount(
        ctx.selectFrom(DSL.table("user_invitation")).where(INVITER_USER_ID.eq(replacementUserId))));
    Assertions.assertEquals(0, ctx.fetchCount(ctx.selectFrom(DSL.table("user_invitation")).where(INVITER_USER_ID.eq(deletedUserId))));
    Assertions.assertEquals(1,
        ctx.fetchCount(ctx.selectFrom(DSL.table("connection_timeline_event")).where(DSL.field("user_id").eq(replacementUserId))));
    Assertions.assertEquals(0, ctx.fetchCount(
        ctx.selectFrom(DSL.table("connection_timeline_event")).where(DSL.field("user_id").eq(deletedUserId))));
  }

  private void assertUserExists(final DSLContext ctx, final UUID userId) {
    Assertions.assertEquals(1, ctx.fetchCount(ctx.selectFrom(USER_TABLE).where(ID.eq(userId))));
  }

  private static void assertNoDuplicateEmails(final DSLContext ctx) {
    Assertions.assertEquals(0, ctx.fetchCount(ctx.select(EMAIL, DSL.count()).from(USER_TABLE).groupBy(EMAIL).having(DSL.count().gt(1))));
  }

  private static void createInvitationFromUser(final DSLContext ctx, final UUID inviterUserId) {
    ctx.insertInto(DSL.table("user_invitation"),
        ID, DSL.field("invite_code"), INVITER_USER_ID, DSL.field("invited_email"), DSL.field("permission_type"),
        DSL.field("status"), DSL.field("scope_id"), DSL.field("scope_type"), DSL.field("expires_at"))
        .values(UUID.randomUUID(), UUID.randomUUID().toString(), inviterUserId, "invited_email", PermissionType.WORKSPACE_ADMIN,
            InvitationStatus.PENDING, UUID.randomUUID(),
            ScopeTypeEnum.workspace, OffsetDateTime.now())
        .execute();
  }

  private static void makeUserSSO(final DSLContext ctx, final UUID userId) {
    final UUID orgId = UUID.randomUUID();
    ctx.insertInto(DSL.table("organization"), DSL.field("id"), DSL.field("name"), DSL.field("email"))
        .values(orgId, "org", "org@airbyte.io")
        .execute();
    ctx.insertInto(DSL.table("sso_config"), DSL.field("id"), DSL.field("organization_id"), DSL.field("keycloak_realm"))
        .values(UUID.randomUUID(), orgId, UUID.randomUUID().toString())
        .execute();
    ctx.insertInto(DSL.table("permission"))
        .columns(ID, DSL.field("user_id"), DSL.field("organization_id"), DSL.field("permission_type"))
        .values(UUID.randomUUID(), userId, orgId, PermissionType.ORGANIZATION_ADMIN)
        .execute();
  }

  private static void createTimelineEventByUser(final DSLContext ctx, final UUID userId) {
    ctx.insertInto(DSL.table("connection_timeline_event"),
        DSL.field("id"), DSL.field("connection_id"), DSL.field("event_type"), DSL.field("user_id"), DSL.field("created_at"))
        .values(UUID.randomUUID(), UUID.randomUUID(), "event_type", userId, OffsetDateTime.now())
        .execute();
  }

  private static void dropTimelineConnectionFK(final DSLContext ctx) {
    ctx.alterTable("connection_timeline_event")
        .dropConstraintIfExists("connection_timeline_event_connection_id_fkey")
        .execute();
  }

}
