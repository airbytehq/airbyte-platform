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
class V1_1_1_002__AddEnterpriseToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addEnterpriseColumn(ctx)
  }

  companion object {
    @JvmStatic
    fun addEnterpriseColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists("enterprise", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
        .execute()
    }
  }
}
