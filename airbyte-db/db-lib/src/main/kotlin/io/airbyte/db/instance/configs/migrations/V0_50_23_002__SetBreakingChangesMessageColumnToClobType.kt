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
 * Changes the message column type of actor_definition_breaking_change to CLOB, which will set\ it
 * to 'text' in the db. We want to be able to handle large messages.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_23_002__SetBreakingChangesMessageColumnToClobType : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    alterMessageColumnType(ctx)
  }

  companion object {
    @JvmStatic
    fun alterMessageColumnType(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_breaking_change")
        .alter("message")
        .set(SQLDataType.CLOB)
        .execute()
    }
  }
}
