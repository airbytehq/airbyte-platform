/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Enforce a 10-minute statement timeout on the current database.
 *
 * This sets the PostgreSQL statement_timeout parameter at the database level
 * so that any session connected to the configApi database will have its
 * statements automatically cancelled after 10 minutes.
 *
 * Rollback: To revert this change, run the following SQL against the configApi database:
 *   ALTER DATABASE "<db_name>" RESET statement_timeout;
 * This removes the database-level override and restores the server default.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_018__EnforceSessionTimeout : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    enforceStatementTimeout(ctx)
  }

  companion object {
    private const val TIMEOUT_MINUTES = 10
    private const val TIMEOUT_MS = TIMEOUT_MINUTES * 60 * 1000

    fun enforceStatementTimeout(ctx: org.jooq.DSLContext) {
      val dbName = ctx.fetchOne("SELECT current_database()")?.getValue(0) as String
      ctx.execute("ALTER DATABASE \"$dbName\" SET statement_timeout = '$TIMEOUT_MS'")
      log.info { "Set statement_timeout to ${TIMEOUT_MS}ms ($TIMEOUT_MINUTES minutes) on database $dbName" }
    }

    private val log = KotlinLogging.logger {}
  }
}
