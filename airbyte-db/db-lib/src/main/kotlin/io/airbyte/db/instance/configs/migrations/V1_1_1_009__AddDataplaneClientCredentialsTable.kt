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
class V1_1_1_009__AddDataplaneClientCredentialsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  companion object {
    private const val DATAPLANE_CLIENT_CREDENTIALS_TABLE = "dataplane_client_credentials"
    private const val DATAPLANE_TABLE = "dataplane"

    fun migrate(ctx: DSLContext) {
      createDataplaneAuthServiceTable(ctx)
    }

    private fun createDataplaneAuthServiceTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val dataplaneId = DSL.field("dataplane_id", SQLDataType.UUID.nullable(false))
      val clientId = DSL.field("client_id", SQLDataType.VARCHAR.nullable(false))
      val clientSecret = DSL.field("client_secret", SQLDataType.VARCHAR.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false))

      ctx
        .createTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .columns(id, dataplaneId, clientId, clientSecret, createdAt, createdBy)
        .constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(dataplaneId).references(DATAPLANE_TABLE, "id").onDeleteCascade(),
          DSL.foreignKey(createdBy).references("user", "id").onDeleteCascade(),
          DSL.unique(dataplaneId, clientId, clientSecret),
        ).execute()
    }
  }
}
