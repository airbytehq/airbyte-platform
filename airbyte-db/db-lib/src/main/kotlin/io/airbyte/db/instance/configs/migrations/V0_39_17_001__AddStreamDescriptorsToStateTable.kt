/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
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
 * Add stream to state table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_39_17_001__AddStreamDescriptorsToStateTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  /**
   * State types.
   */
  enum class StateType(
    private val literal: String,
  ) : EnumType {
    GLOBAL("GLOBAL"),
    STREAM("STREAM"),
    LEGACY("LEGACY"),
    ;

    override fun getLiteral(): String = literal

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    companion object {
      const val NAME: String = "state_type"
    }
  }

  companion object {
    private const val STATE_TABLE = "state"

    @VisibleForTesting
    fun migrate(ctx: DSLContext) {
      createStateTypeEnum(ctx)
      addStreamDescriptorFieldsToStateTable(ctx)
    }

    private fun createStateTypeEnum(ctx: DSLContext) {
      ctx
        .createType(StateType.NAME)
        .asEnum(*StateType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun addStreamDescriptorFieldsToStateTable(ctx: DSLContext) {
      ctx
        .alterTable(STATE_TABLE)
        .add(
          listOf(
            DSL.field("stream_name", SQLDataType.CLOB.nullable(true)),
            DSL.field("namespace", SQLDataType.CLOB.nullable(true)), // type defaults to LEGACY to first set the expected type of all existing states
            DSL.field(
              "type",
              SQLDataType.VARCHAR
                .asEnumDataType(StateType::class.java)
                .nullable(false)
                .defaultValue(StateType.LEGACY),
            ),
            DSL
              .constraint(
                "state__connection_id__stream_name__namespace__uq",
              ).unique(DSL.field("connection_id"), DSL.field("stream_name"), DSL.field("namespace")),
          ),
        ).execute()
    }
  }
}
