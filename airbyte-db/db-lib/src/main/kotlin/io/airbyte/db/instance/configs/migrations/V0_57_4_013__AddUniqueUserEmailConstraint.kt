/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Record1
import org.jooq.Record3
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.time.OffsetDateTime
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_4_013__AddUniqueUserEmailConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    deleteDuplicateUsers(ctx)
    addUniqueUserEmailConstraint(ctx)
  }

  @JvmRecord
  internal data class User(
    val id: UUID,
    val email: String,
    val createdAt: OffsetDateTime,
  )

  companion object {
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val USER = DSL.table("\"user\"")
    private val USER_INVITATION = DSL.table("user_invitation")
    private val CONNECTION_TIMELINE_EVENT = DSL.table("connection_timeline_event")
    private val SSO_CONFIG = DSL.table("sso_config")
    private val PERMISSION = DSL.table("permission")
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val USER_ID = DSL.field("user_id", SQLDataType.UUID)
    private val INVITER_USER_ID = DSL.field("inviter_user_id", SQLDataType.UUID)
    private val ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID)
    private val CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val EMAIL = DSL.field("email", SQLDataType.VARCHAR)

    @JvmStatic
    @VisibleForTesting
    fun deleteDuplicateUsers(ctx: DSLContext) {
      val duplicateEmails = getDuplicateEmails(ctx)
      val ssoOrganizationIds = getSsoOrganizationIds(ctx)

      for (email in duplicateEmails) {
        log.info { "Found duplicate users with email $email" }
        var users = getUsersByEmail(ctx, email)

        val defaultUser =
          users.firstOrNull { it.id == DEFAULT_USER_ID }

        defaultUser?.let {
          log.info { "Clearing email for default user" }
          clearUserEmail(ctx, DEFAULT_USER_ID)
          users = getUsersByEmail(ctx, email)
        }

        if (users.size > 1) {
          val userToKeep = getUserToKeep(ctx, users, ssoOrganizationIds)
          users
            .filter { u: User -> u.id != userToKeep }
            .forEach { u: User -> deleteUserById(ctx, u.id, userToKeep) }
        }
      }
    }

    private fun getUserToKeep(
      ctx: DSLContext,
      users: List<User>,
      ssoOrganizationIds: List<UUID?>,
    ): UUID {
      // Prefer to keep user with permissions to an SSO organization
      val ssoUsers =
        users
          .filter { u: User -> hasSsoOrgPermissions(ctx, u.id, ssoOrganizationIds) }
          .toList()

      if (ssoUsers.size == 1) {
        log.info { "Keeping user with SSO permissions ${ssoUsers.first()}" }
        return ssoUsers.first().id
      }

      // Otherwise, keep the oldest one
      val oldestUserId =
        users
          .minByOrNull { it.createdAt }
          ?.id
          ?: throw NullPointerException()

      log.info { "Keeping oldest user $oldestUserId" }
      return oldestUserId
    }

    private fun deleteUserById(
      ctx: DSLContext,
      userId: UUID,
      replacementUserId: UUID,
    ) {
      // update sent invitations
      val inviteUpdateCount =
        ctx
          .update(USER_INVITATION)
          .set(INVITER_USER_ID, replacementUserId)
          .where(INVITER_USER_ID.eq(userId))
          .execute()
      if (inviteUpdateCount > 0) {
        log.info { "Updated $inviteUpdateCount user invitations from $userId with inviter_user_id $replacementUserId" }
      }

      // update timeline events
      val eventUpdateCount =
        ctx
          .update(CONNECTION_TIMELINE_EVENT)
          .set(USER_ID, replacementUserId)
          .where(USER_ID.eq(userId))
          .execute()
      if (eventUpdateCount > 0) {
        log.info { "Updated $eventUpdateCount connection timeline events from $userId with user_id $replacementUserId" }
      }

      // delete user
      ctx
        .deleteFrom(USER)
        .where(ID.eq(userId))
        .execute()
      log.info { "Deleted user with id $userId" }
    }

    private fun getSsoOrganizationIds(ctx: DSLContext): List<UUID?> =
      ctx
        .select(ORGANIZATION_ID)
        .from(SSO_CONFIG)
        .fetch()
        .map { r: Record1<UUID> ->
          r.get(
            ORGANIZATION_ID,
          )
        }

    private fun hasSsoOrgPermissions(
      ctx: DSLContext,
      userId: UUID,
      ssoOrganizationIds: List<UUID?>,
    ): Boolean {
      val permissions =
        ctx
          .select(DSL.asterisk())
          .from(PERMISSION)
          .where(USER_ID.eq(userId).and(ORGANIZATION_ID.`in`(ssoOrganizationIds)))
          .fetch()
      return permissions.isNotEmpty
    }

    private fun clearUserEmail(
      ctx: DSLContext,
      userId: UUID,
    ) {
      ctx
        .update(USER)
        .set(EMAIL, "")
        .where(ID.eq(userId))
        .execute()
    }

    private fun getDuplicateEmails(ctx: DSLContext): List<String> =
      ctx
        .select(EMAIL)
        .from(USER)
        .groupBy(EMAIL)
        .having(DSL.count().greaterThan(1))
        .fetch()
        .map { r: Record1<String> ->
          r.get(
            EMAIL,
          )
        }

    private fun getUsersByEmail(
      ctx: DSLContext,
      email: String,
    ): List<User> =
      ctx
        .select(ID, EMAIL, CREATED_AT)
        .from(USER)
        .where(EMAIL.eq(email))
        .fetch()
        .map { r: Record3<UUID, String, OffsetDateTime> ->
          User(
            r.get(ID),
            r.get(EMAIL),
            r.get(CREATED_AT),
          )
        }

    @JvmStatic
    @VisibleForTesting
    fun addUniqueUserEmailConstraint(ctx: DSLContext) {
      ctx
        .createUniqueIndex("user_email_unique_key")
        .on(USER, DSL.lower(EMAIL))
        .execute()
    }
  }
}
