/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.Companion.dropConstraintIfExists
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatchTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_64_4_002__AddJobRunnerPermissionTypesTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_64_4_002__AddJobRunnerPermissionTypes()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
    val ctx = dslContext!!
    ctx.deleteFrom(USER_INVITATION_TABLE).execute()
  }

  @Test
  fun testRemoveInvalidUserInvitation() {
    val ctx = dslContext!!
    dropConstraintIfExists(ctx)
    createUserInvitation(
      ctx,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.WORKSPACE_ADMIN,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.organization,
    )
    createUserInvitation(
      ctx,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.ORGANIZATION_ADMIN,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.workspace,
    )
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx)
    val result =
      ctx
        .select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch()

    Assertions.assertEquals(0, result.size)
  }

  @Test
  fun testDoesNotRemoveValidUserInvitation() {
    val ctx = dslContext!!
    createUserInvitation(
      ctx,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.WORKSPACE_ADMIN,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.workspace,
    )
    createUserInvitation(
      ctx,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.ORGANIZATION_ADMIN,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.organization,
    )
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx)
    val result =
      ctx
        .select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch()

    Assertions.assertEquals(2, result.size)
  }

  @Test
  fun testPreventsInsertionOfInvalidInvites() {
    val ctx = dslContext!!
    V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.runMigration(ctx)
    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      createUserInvitation(
        ctx,
        V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.WORKSPACE_ADMIN,
        V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.organization,
      )
    }
    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      createUserInvitation(
        ctx,
        V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.ORGANIZATION_ADMIN,
        V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.workspace,
      )
    }
    createUserInvitation(
      ctx,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType.ORGANIZATION_ADMIN,
      V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum.organization,
    )
    val result =
      ctx
        .select(DSL.asterisk())
        .from(USER_INVITATION_TABLE)
        .fetch()

    Assertions.assertEquals(1, result.size)
  }

  fun createUserInvitation(
    ctx: DSLContext,
    permissionType: V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.PermissionType,
    scopeType: V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum,
  ) {
    val inviteCode = UUID.randomUUID().toString()
    val invitedEmail = UUID.randomUUID().toString() + "@test.com"
    val status =
      V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.InvitationStatus.PENDING // Status doesn't matter for these tests
    val scopeId = UUID.randomUUID()
    val expiresAt = Timestamp(System.currentTimeMillis()) // expiresAt doesn't matter for these tests.
    ctx
      .insertInto(USER_INVITATION_TABLE)
      .columns(
        DSL.field("id"),
        DSL.field("inviter_user_id"),
        DSL.field("invite_code"),
        DSL.field("invited_email"),
        DSL.field(
          "status",
          SQLDataType.VARCHAR.asEnumDataType(
            V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.InvitationStatus::class.java,
          ),
        ),
        DSL.field("permission_type"),
        DSL.field(
          "scope_type",
          SQLDataType.VARCHAR.asEnumDataType(
            V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.ScopeTypeEnum::class.java,
          ),
        ),
        DSL.field("scope_id"),
        DSL.field("expires_at"),
      ).values(
        UUID.randomUUID(),
        DEFAULT_USER_ID,
        inviteCode,
        invitedEmail,
        status,
        permissionType,
        scopeType,
        scopeId,
        expiresAt,
      ).execute()
  }

  companion object {
    private val USER_INVITATION_TABLE = DSL.table("user_invitation")
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
