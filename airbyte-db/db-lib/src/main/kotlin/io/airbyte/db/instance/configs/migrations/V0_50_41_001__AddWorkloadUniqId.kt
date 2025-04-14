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
class V0_50_41_001__AddWorkloadUniqId : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    val autoIdField = DSL.field(AUTO_ID, SQLDataType.UUID.nullable(true))
    ctx
      .alterTable(TABLE)
      .addColumnIfNotExists(autoIdField)
      .execute()
  }

  companion object {
    private const val TABLE = "workload"
    private const val AUTO_ID = "auto_id"
  }
}
