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
class V0_55_1_002__AddGenerationTable : BaseJavaMigration() {
  @Suppress("ktlint:standard:class-naming")
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createGenerationTable(ctx)
  }

  companion object {
    const val STREAM_GENERATION_TABLE_NAME: String = "stream_generation"

    @JvmStatic
    fun createGenerationTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false))
      val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(true))
      val generationId = DSL.field("generation_id", SQLDataType.BIGINT.nullable(false))
      val startJobId = DSL.field("start_job_id", SQLDataType.BIGINT.nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTable(STREAM_GENERATION_TABLE_NAME)
        .columns(
          id,
          connectionId,
          streamName,
          streamNamespace,
          generationId,
          startJobId,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
        ).execute()

      val indexCreationQuery =
        "CREATE INDEX ON $STREAM_GENERATION_TABLE_NAME USING btree (${connectionId.name}, ${streamName.name}, ${generationId.name} DESC)"

      val indexCreationQuery2 =
        "CREATE INDEX ON $STREAM_GENERATION_TABLE_NAME USING btree (${connectionId.name}, ${streamName.name}, ${streamNamespace.name}, ${generationId.name} DESC)"

      ctx.execute(indexCreationQuery)
      ctx.execute(indexCreationQuery2)
    }
  }
}
