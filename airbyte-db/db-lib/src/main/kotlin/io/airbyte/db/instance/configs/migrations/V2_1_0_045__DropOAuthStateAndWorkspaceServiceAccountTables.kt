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
 * Drops the unused `oauth_state` and `workspace_service_account` tables. No application code reads
 * or writes either table; public API OAuth state is stored in Redis.
 *
 * Not reversible: the dropped rows are lost. Restoring the tables requires a new forward migration
 * that recreates them empty; never edit or remove an applied migration.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_045__DropOAuthStateAndWorkspaceServiceAccountTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute("DROP TABLE IF EXISTS oauth_state")
    ctx.execute("DROP TABLE IF EXISTS workspace_service_account")
  }
}
