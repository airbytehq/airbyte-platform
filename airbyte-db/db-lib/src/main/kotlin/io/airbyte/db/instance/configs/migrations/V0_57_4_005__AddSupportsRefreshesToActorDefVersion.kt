/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_4_005__AddSupportsRefreshesToActorDefVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    ctx
      .alterTable("actor_definition_version")
      .addColumn("supports_refreshes", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
      .execute()

    ctx
      .alterTable("actor_definition")
      .dropColumn("support_refreshes")
      .execute()
  }
}
