/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Migration to add auth_refresh_token table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_57_4_010__AddAuthRefreshToken : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    createAuthRefreshTokenTable(ctx)
  }

  companion object {
    private const val AUTH_REFRESH_TOKEN_TABLE = "auth_refresh_token"
    private val sessionId = DSL.field("session_id", SQLDataType.VARCHAR.nullable(false))
    private val value = DSL.field("value", SQLDataType.VARCHAR.nullable(false))
    private val revoked = DSL.field("revoked", SQLDataType.BOOLEAN.nullable(false).defaultValue(true))
    private val createdAt =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
    private val updatedAt =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    fun createAuthRefreshTokenTable(ctx: DSLContext) {
      ctx
        .createTable(AUTH_REFRESH_TOKEN_TABLE)
        .columns(
          value,
          sessionId,
          revoked,
          createdAt,
          updatedAt,
        ).constraints(DSL.primaryKey(value), DSL.unique(sessionId, value))
        .execute()
    }
  }
}
