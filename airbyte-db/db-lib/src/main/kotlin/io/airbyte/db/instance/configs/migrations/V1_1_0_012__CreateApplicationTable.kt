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
class V1_1_0_012__CreateApplicationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val userId = DSL.field("user_id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR.nullable(false))
    val clientId = DSL.field("client_id", SQLDataType.VARCHAR.nullable(false))
    val clientSecret = DSL.field("client_secret", SQLDataType.VARCHAR.nullable(false))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    val ctx = DSL.using(context.connection)

    ctx
      .createTable(APPLICATION_TABLE)
      .columns(
        id,
        userId,
        name,
        clientId,
        clientSecret,
        createdAt,
      ).execute()
  }

  companion object {
    private const val APPLICATION_TABLE = "application"
  }
}
