/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.InvitationStatus;
import io.airbyte.db.instance.configs.migrations.V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType;
import io.airbyte.db.instance.configs.migrations.V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.sql.Timestamp;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatchTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> USER_INVITATION_TABLE = DSL.table("user_invitation");
  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_64_4_002__AddJobRunnerPermissionTypesTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_64_4_002__AddJobRunnerPermissionTypes();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
    final DSLContext ctx = getDslContext();
    ctx.deleteFrom(USER_INVITATION_TABLE).execute();
  }

  @Test
  void testRemoveInvalidUserInvitation() {
    final DSLContext ctx = getDslContext();
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.dropConstraintIfExists(ctx);
    createUserInvitation(ctx, PermissionType.WORKSPACE_ADMIN, ScopeTypeEnum.organization);
    createUserInvitation(ctx, PermissionType.ORGANIZATION_ADMIN, ScopeTypeEnum.workspace);
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx);
    final Result<Record> result = ctx.select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch();

    Assertions.assertEquals(0, result.size());
  }

  @Test
  void testDoesNotRemoveValidUserInvitation() {
    final DSLContext ctx = getDslContext();
    createUserInvitation(ctx, PermissionType.WORKSPACE_ADMIN, ScopeTypeEnum.workspace);
    createUserInvitation(ctx, PermissionType.ORGANIZATION_ADMIN, ScopeTypeEnum.organization);
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx);
    final Result<Record> result = ctx.select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch();

    Assertions.assertEquals(2, result.size());
  }

  @Test
  void testPreventsInsertionOfInvalidInvites() {
    final DSLContext ctx = getDslContext();
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx);
    Assertions.assertThrows(IntegrityConstraintViolationException.class,
        () -> createUserInvitation(ctx, PermissionType.WORKSPACE_ADMIN, ScopeTypeEnum.organization));
    Assertions.assertThrows(IntegrityConstraintViolationException.class,
        () -> createUserInvitation(ctx, PermissionType.ORGANIZATION_ADMIN, ScopeTypeEnum.workspace));
    createUserInvitation(ctx, PermissionType.ORGANIZATION_ADMIN, ScopeTypeEnum.organization);
    Result<Record> result = ctx.select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch();

    Assertions.assertEquals(1, result.size());
  }

  void createUserInvitation(final DSLContext ctx, final PermissionType permissionType, final ScopeTypeEnum scopeType) {
    final String inviteCode = UUID.randomUUID().toString();
    final String invitedEmail = UUID.randomUUID() + "@test.com";
    final InvitationStatus status = InvitationStatus.PENDING; // Status doesn't matter for these tests
    final UUID scopeId = UUID.randomUUID();
    final Timestamp expiresAt = new Timestamp(System.currentTimeMillis()); // expiresAt doesn't matter for these tests.
    ctx.insertInto(USER_INVITATION_TABLE)
        .columns(
            DSL.field("id"),
            DSL.field("inviter_user_id"),
            DSL.field("invite_code"),
            DSL.field("invited_email"),
            DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(InvitationStatus.class)),
            DSL.field("permission_type"),
            DSL.field("scope_type", SQLDataType.VARCHAR.asEnumDataType(ScopeTypeEnum.class)),
            DSL.field("scope_id"),
            DSL.field("expires_at"))
        .values(UUID.randomUUID(), DEFAULT_USER_ID, inviteCode, invitedEmail, status, permissionType, scopeType, scopeId, expiresAt)
        .execute();
  }

}
