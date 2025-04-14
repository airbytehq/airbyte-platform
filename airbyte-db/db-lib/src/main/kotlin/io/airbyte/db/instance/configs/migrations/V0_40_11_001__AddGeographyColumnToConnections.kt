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
 * Add geography to connection migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_11_001__AddGeographyColumnToConnections : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    addGeographyEnumDataTypes(ctx)
    addGeographyColumnToConnection(ctx)
    addGeographyColumnToWorkspace(ctx)
  }

  /**
   * Supported geographies.
   */
  enum class GeographyType(
    private val literal: String,
  ) : EnumType {
    AUTO("AUTO"),
    US("US"),
    EU("EU"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "geography_type"
    }
  }

  companion object {
    private fun addGeographyEnumDataTypes(ctx: DSLContext) {
      ctx
        .createType(GeographyType.NAME)
        .asEnum(*GeographyType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun addGeographyColumnToConnection(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field(
            "geography",
            SQLDataType.VARCHAR
              .asEnumDataType(GeographyType::class.java)
              .nullable(false)
              .defaultValue(GeographyType.AUTO),
          ),
        ).execute()
    }

    private fun addGeographyColumnToWorkspace(ctx: DSLContext) {
      ctx
        .alterTable("workspace")
        .addColumnIfNotExists(
          DSL.field(
            "geography",
            SQLDataType.VARCHAR
              .asEnumDataType(
                GeographyType::class.java,
              ).nullable(false)
              .defaultValue(GeographyType.AUTO),
          ),
        ).execute()
    }
  }
}
