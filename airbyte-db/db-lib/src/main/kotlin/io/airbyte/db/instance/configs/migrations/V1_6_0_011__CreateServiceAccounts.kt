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
class V1_6_0_011__CreateServiceAccounts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    val name = DSL.field("name", SQLDataType.VARCHAR.nullable(false))
    val secret = DSL.field("secret", SQLDataType.VARCHAR.nullable(false))
    val managed = DSL.field("managed", SQLDataType.BOOLEAN.nullable(false))
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    // Create service_accounts table
    ctx
      .createTableIfNotExists("service_accounts")
      .columns(id, name, secret, managed, createdAt, updatedAt)
      .constraints(
        DSL.primaryKey(id),
      ).execute()
  }
}
