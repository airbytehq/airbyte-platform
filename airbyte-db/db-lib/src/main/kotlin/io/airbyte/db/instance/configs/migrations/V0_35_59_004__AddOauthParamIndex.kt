/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add OAuth params index migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_59_004__AddOauthParamIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx
      .createIndexIfNotExists("actor_oauth_parameter_workspace_definition_idx")
      .on("actor_oauth_parameter", "workspace_id", "actor_definition_id")
      .execute()
  }
}
