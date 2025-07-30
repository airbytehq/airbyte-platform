/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Remove actor foreign key from oauth params table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_3_002__RemoveActorForeignKeyFromOauthParamsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    removeActorDefinitionForeignKey(ctx)
  }

  companion object {
    @JvmStatic
    fun removeActorDefinitionForeignKey(ctx: DSLContext) {
      ctx
        .alterTable("actor_oauth_parameter")
        .dropForeignKey("actor_oauth_parameter_actor_definition_id_fkey")
        .execute()
    }
  }
}
