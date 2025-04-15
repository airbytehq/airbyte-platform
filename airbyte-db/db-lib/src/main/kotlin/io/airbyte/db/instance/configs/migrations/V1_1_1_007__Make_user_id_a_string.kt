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

@Suppress("ktlint:standard:class-naming")
class V1_1_1_007__Make_user_id_a_string : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    alterUserIdToBeAString(ctx)
  }

  companion object {
    fun alterUserIdToBeAString(ctx: DSLContext) {
      ctx
        .alterTable("application")
        .alterColumn("user_id")
        .set(SQLDataType.CLOB)
        .execute()

      ctx
        .alterTable("application")
        .renameColumn("user_id")
        .to("auth_user_id")
        .execute()
    }
  }
}
