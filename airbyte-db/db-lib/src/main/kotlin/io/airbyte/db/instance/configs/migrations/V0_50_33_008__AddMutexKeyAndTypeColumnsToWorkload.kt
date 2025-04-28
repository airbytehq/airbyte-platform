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

@Suppress("ktlint:standard:class-naming")
class V0_50_33_008__AddMutexKeyAndTypeColumnsToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    val mutexKeyColumn = DSL.field(MUTEX_KEY_COLUMN_NAME, SQLDataType.VARCHAR.nullable(true))
    ctx
      .alterTable(TABLE)
      .addColumnIfNotExists(mutexKeyColumn)
      .execute()

    createWorkloadTypeEnum(ctx)

    val typeColumn =
      DSL.field(
        TYPE_COLUMN_NAME,
        SQLDataType.VARCHAR
          .asEnumDataType(
            WorkloadType::class.java,
          ).nullable(false),
      )
    ctx
      .alterTable(TABLE)
      .addColumnIfNotExists(typeColumn)
      .execute()
  }

  internal enum class WorkloadType(
    private val literal: String,
  ) : EnumType {
    SYNC("sync"),
    CHECK("check"),
    DISCOVER("discover"),
    SPEC("spec"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = WORKLOAD_TYPE_ENUM_NAME

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val TABLE = "workload"
    private const val MUTEX_KEY_COLUMN_NAME = "mutex_key"
    private const val TYPE_COLUMN_NAME = "type"
    private const val WORKLOAD_TYPE_ENUM_NAME = "workload_type"

    private fun createWorkloadTypeEnum(ctx: DSLContext) {
      ctx
        .createType(WORKLOAD_TYPE_ENUM_NAME)
        .asEnum(
          WorkloadType.SYNC.literal,
          WorkloadType.CHECK.literal,
          WorkloadType.DISCOVER.literal,
          WorkloadType.SPEC.literal,
        ).execute()
    }
  }
}
