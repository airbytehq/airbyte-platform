/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add accepted_by_user_id column and expires_at column to user_invitations table. Also add expired
 * status to invitation_status enum.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_41_011__AddUserInvitationAcceptedByAndExpiration : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    addAcceptedByUserIdColumnAndIndex(ctx)
    addExpiresAtColumnAndIndex(ctx)
    addExpiredStatus(ctx)
  }

  companion object {
    private const val ACCEPTED_BY_USER_ID = "accepted_by_user_id"
    private const val EXPIRES_AT = "expires_at"
    private const val INVITATION_STATUS = "invitation_status"
    private const val EXPIRED = "expired"

    private val ACCEPTED_BY_USER_ID_COLUMN = DSL.field(ACCEPTED_BY_USER_ID, SQLDataType.UUID.nullable(true))
    private val EXPIRES_AT_COLUMN = DSL.field(EXPIRES_AT, SQLDataType.TIMESTAMP.nullable(false))

    fun addAcceptedByUserIdColumnAndIndex(ctx: DSLContext) {
      ctx
        .alterTable(USER_INVITATION_TABLE)
        .addColumnIfNotExists(ACCEPTED_BY_USER_ID_COLUMN)
        .execute()

      ctx
        .alterTable(USER_INVITATION_TABLE)
        .add(
          DSL
            .foreignKey(ACCEPTED_BY_USER_ID)
            .references(USER_TABLE, "id")
            .onDeleteCascade(),
        ).execute()

      ctx
        .createIndex("user_invitation_accepted_by_user_id_index")
        .on(USER_INVITATION_TABLE, ACCEPTED_BY_USER_ID)
        .execute()
    }

    fun addExpiresAtColumnAndIndex(ctx: DSLContext) {
      ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(EXPIRES_AT_COLUMN).execute()

      ctx
        .createIndex("user_invitation_expires_at_index")
        .on(USER_INVITATION_TABLE, EXPIRES_AT)
        .execute()
    }

    fun addExpiredStatus(ctx: DSLContext) {
      ctx.alterType(INVITATION_STATUS).addValue(EXPIRED).execute()
    }
  }
}
