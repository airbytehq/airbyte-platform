/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_57_4_013__AddUniqueUserEmailConstraint.Companion.addUniqueUserEmailConstraint
import io.airbyte.db.instance.configs.migrations.V0_57_4_013__AddUniqueUserEmailConstraint.Companion.deleteDuplicateUsers
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.time.OffsetDateTime
import java.util.UUID

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_57_4_013__AddUniqueUserEmailConstraintTest : AbstractConfigsDatabaseTest() {
  private var email: String? = null

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_57_4_013__AddUniqueUserEmailConstraintTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    email = UUID.randomUUID().toString() + "@airbyte.io"

    // Remove constraint preventing timeline event creation
    dropTimelineConnectionFK(getDslContext())
  }

  @Test
  @Order(10)
  fun testSSOUserMigration() {
    val ctx = getDslContext()

    val ssoUserId = UUID.randomUUID()
    createUser(ctx, ssoUserId, email, OffsetDateTime.now())
    makeUserSSO(ctx, ssoUserId)

    val userId = UUID.randomUUID()
    createUser(ctx, userId, email, OffsetDateTime.now().minusDays(1))
    createInvitationFromUser(ctx, userId)
    createTimelineEventByUser(ctx, userId)

    deleteDuplicateUsers(ctx)

    assertUserReplaced(ctx, userId, ssoUserId)
    assertNoDuplicateEmails(ctx)
  }

  @Test
  @Order(10)
  fun testMoreThanOneSSOUserKeepsOldest() {
    val ctx = getDslContext()

    val ssoUserId = UUID.randomUUID()
    createUser(ctx, ssoUserId, email, OffsetDateTime.now())
    createInvitationFromUser(ctx, ssoUserId)
    createTimelineEventByUser(ctx, ssoUserId)
    makeUserSSO(ctx, ssoUserId)

    val ssoUserId2 = UUID.randomUUID()
    createUser(ctx, ssoUserId2, email, OffsetDateTime.now().minusDays(1))
    makeUserSSO(ctx, ssoUserId2)

    deleteDuplicateUsers(ctx)

    assertUserReplaced(ctx, ssoUserId, ssoUserId2)
    assertNoDuplicateEmails(ctx)
  }

  @Test
  @Order(10)
  fun testNonSSOKeepOldestUser() {
    val ctx = getDslContext()

    val userId = UUID.randomUUID()
    createUser(ctx, userId, email, OffsetDateTime.now())
    createInvitationFromUser(ctx, userId)
    createTimelineEventByUser(ctx, userId)

    val userId2 = UUID.randomUUID()
    createUser(ctx, userId2, email, OffsetDateTime.now().minusDays(1))

    deleteDuplicateUsers(ctx)

    assertUserReplaced(ctx, userId, userId2)
    assertNoDuplicateEmails(ctx)
  }

  @Test
  @Order(10)
  fun testUnsetDefaultUserEmail() {
    val ctx = getDslContext()

    ctx
      .update(USER_TABLE)
      .set(EMAIL, email)
      .where(ID.eq(DEFAULT_USER_ID))
      .execute()

    val userId2 = UUID.randomUUID()
    createUser(ctx, userId2, email, OffsetDateTime.now())
    createInvitationFromUser(ctx, userId2)
    createTimelineEventByUser(ctx, userId2)

    deleteDuplicateUsers(ctx)

    assertUserExists(ctx, DEFAULT_USER_ID)
    assertUserExists(ctx, userId2)
    assertNoDuplicateEmails(ctx)

    val defaultUserEmail =
      ctx
        .select(EMAIL)
        .from(USER_TABLE)
        .where(ID.eq(DEFAULT_USER_ID))
        .fetchOptional()
    Assertions.assertTrue(defaultUserEmail.isPresent)
    Assertions.assertEquals("", defaultUserEmail.get().value1())
  }

  @Test
  @Order(100)
  fun testUniqueConstraint() {
    val ctx = getDslContext()
    addUniqueUserEmailConstraint(ctx)

    val email = "bob@airbyte.io"

    createUser(ctx, UUID.randomUUID(), email, OffsetDateTime.now())
    Assertions.assertThrows(
      Exception::class.java,
    ) {
      createUser(
        ctx,
        UUID.randomUUID(),
        email,
        OffsetDateTime.now(),
      )
    }

    val alternateCasingEmail = "BOB@airbyte.io"
    Assertions.assertThrows(
      Exception::class.java,
    ) {
      createUser(
        ctx,
        UUID.randomUUID(),
        alternateCasingEmail,
        OffsetDateTime.now(),
      )
    }

    createUser(ctx, UUID.randomUUID(), "anotherone@airbyte.io", OffsetDateTime.now())

    assertNoDuplicateEmails(ctx)
  }

  private fun assertUserReplaced(
    ctx: DSLContext,
    deletedUserId: UUID,
    replacementUserId: UUID,
  ) {
    assertUserExists(ctx, replacementUserId)
    Assertions.assertEquals(0, ctx.fetchCount(ctx.selectFrom(USER_TABLE).where(ID.eq(deletedUserId))))
    Assertions.assertEquals(
      1,
      ctx.fetchCount(
        ctx.selectFrom(DSL.table("user_invitation")).where(INVITER_USER_ID.eq(replacementUserId)),
      ),
    )
    Assertions.assertEquals(
      0,
      ctx.fetchCount(
        ctx.selectFrom(DSL.table("user_invitation")).where(
          INVITER_USER_ID.eq(deletedUserId),
        ),
      ),
    )
    Assertions.assertEquals(
      1,
      ctx.fetchCount(
        ctx.selectFrom(DSL.table("connection_timeline_event")).where(DSL.field("user_id").eq(replacementUserId)),
      ),
    )
    Assertions.assertEquals(
      0,
      ctx.fetchCount(
        ctx.selectFrom(DSL.table("connection_timeline_event")).where(DSL.field("user_id").eq(deletedUserId)),
      ),
    )
  }

  private fun assertUserExists(
    ctx: DSLContext,
    userId: UUID,
  ) {
    Assertions.assertEquals(1, ctx.fetchCount(ctx.selectFrom(USER_TABLE).where(ID.eq(userId))))
  }

  companion object {
    private val USER_TABLE = DSL.table("\"user\"")
    private val EMAIL = DSL.field("email", SQLDataType.VARCHAR)
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val INVITER_USER_ID = DSL.field("inviter_user_id", SQLDataType.UUID)
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    private fun createUser(
      ctx: DSLContext,
      id: UUID,
      email: String?,
      createdAt: OffsetDateTime,
    ) {
      ctx
        .insertInto(USER_TABLE, ID, EMAIL, DSL.field("name"), CREATED_AT)
        .values(id, email, "Name", createdAt)
        .execute()
    }

    private fun assertNoDuplicateEmails(ctx: DSLContext) {
      Assertions.assertEquals(
        0,
        ctx.fetchCount(
          ctx
            .select(EMAIL, DSL.count())
            .from(
              USER_TABLE,
            ).groupBy(EMAIL)
            .having(DSL.count().gt(1)),
        ),
      )
    }

    private fun createInvitationFromUser(
      ctx: DSLContext,
      inviterUserId: UUID,
    ) {
      ctx
        .insertInto(
          DSL.table("user_invitation"),
          ID,
          DSL.field("invite_code"),
          INVITER_USER_ID,
          DSL.field("invited_email"),
          DSL.field("permission_type"),
          DSL.field("status"),
          DSL.field("scope_id"),
          DSL.field("scope_type"),
          DSL.field("expires_at"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID().toString(),
          inviterUserId,
          "invited_email",
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.WORKSPACE_ADMIN,
          V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.InvitationStatus.PENDING,
          UUID.randomUUID(),
          V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.workspace,
          OffsetDateTime.now(),
        ).execute()
    }

    private fun makeUserSSO(
      ctx: DSLContext,
      userId: UUID,
    ) {
      val orgId = UUID.randomUUID()
      ctx
        .insertInto(DSL.table("organization"), DSL.field("id"), DSL.field("name"), DSL.field("email"))
        .values(orgId, "org", "org@airbyte.io")
        .execute()
      ctx
        .insertInto(
          DSL.table("sso_config"),
          DSL.field("id"),
          DSL.field("organization_id"),
          DSL.field("keycloak_realm"),
        ).values(UUID.randomUUID(), orgId, UUID.randomUUID().toString())
        .execute()
      ctx
        .insertInto(DSL.table("permission"))
        .columns(ID, DSL.field("user_id"), DSL.field("organization_id"), DSL.field("permission_type"))
        .values(
          UUID.randomUUID(),
          userId,
          orgId,
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_ADMIN,
        ).execute()
    }

    private fun createTimelineEventByUser(
      ctx: DSLContext,
      userId: UUID,
    ) {
      ctx
        .insertInto(
          DSL.table("connection_timeline_event"),
          DSL.field("id"),
          DSL.field("connection_id"),
          DSL.field("event_type"),
          DSL.field("user_id"),
          DSL.field("created_at"),
        ).values(UUID.randomUUID(), UUID.randomUUID(), "event_type", userId, OffsetDateTime.now())
        .execute()
    }

    private fun dropTimelineConnectionFK(ctx: DSLContext) {
      ctx
        .alterTable("connection_timeline_event")
        .dropConstraintIfExists("connection_timeline_event_connection_id_fkey")
        .execute()
    }
  }
}
