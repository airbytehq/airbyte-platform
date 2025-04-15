/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

/**
 * Add schedule type to configs table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_36_3_001__AddScheduleTypeToConfigsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createScheduleTypeEnum(ctx)
    addPublicColumn(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class ScheduleType(
    private val literal: String,
  ) : EnumType {
    manual("manual"),
    basicSchedule("basic_schedule"),
    cron("cron"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "schedule_type"

    override fun getLiteral(): String = literal
  }

  companion object {
    private fun createScheduleTypeEnum(ctx: DSLContext) {
      ctx.createType("schedule_type").asEnum("manual", "basic_schedule", "cron").execute()
    }

    private fun addPublicColumn(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field(
            "schedule_type",
            SQLDataType.VARCHAR
              .asEnumDataType(
                ScheduleType::class.java,
              ).nullable(true),
          ),
        ).execute()
    }
  }
}
