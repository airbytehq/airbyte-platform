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

@Suppress("ktlint:standard:class-naming")
class V1_1_0_004__UpdateConfigOriginTypeEnum : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    private const val SCOPED_CONFIGURATION_TABLE = "scoped_configuration"
    private const val ORIGIN_TYPE_COLUMN = "origin_type"
    private const val CONFIG_ORIGIN_TYPE = "config_origin_type"
    private const val RELEASE_CANDIDATE = "release_candidate"
    private const val CONNECTOR_ROLLOUT = "connector_rollout"

    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      ctx
        .alterType(CONFIG_ORIGIN_TYPE)
        .renameValue(RELEASE_CANDIDATE)
        .to(CONNECTOR_ROLLOUT)
        .execute()
      log.info { "Updated from '$RELEASE_CANDIDATE' to '$CONNECTOR_ROLLOUT' in table '$SCOPED_CONFIGURATION_TABLE' column '$ORIGIN_TYPE_COLUMN'" }
    }
  }
}
