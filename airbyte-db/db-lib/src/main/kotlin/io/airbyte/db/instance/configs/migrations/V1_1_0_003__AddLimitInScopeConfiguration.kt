/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_0_003__AddLimitInScopeConfiguration : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    val newEnumValues = listOf("user", "workspace", "connection", "source", "destination")
    for (enumValue in newEnumValues) {
      ctx
        .alterType("config_resource_type")
        .addValue(enumValue)
        .execute()
    }
  }
}
