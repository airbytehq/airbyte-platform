/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE
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
 * Modify the schema management table to support backfill preferences.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_41_003__AddBackfillConfigToSchemaManagementTable : BaseJavaMigration() {
  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class BackfillPreference(
    private val literal: String,
  ) : EnumType {
    enabled(ENABLED),
    disabled(DISABLED),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = BACKFILL_PREFERENCE

    override fun getLiteral(): String = literal
  }

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addBackfillTypeEnum(ctx)
    addBackfillPreferenceColumnToSchemaManagementTable(ctx)
  }

  companion object {
    private const val BACKFILL_PREFERENCE = "backfill_preference"
    private const val ENABLED = "enabled"
    private const val DISABLED = "disabled"

    private val BACKFILL_PREFERENCE_COLUMN =
      DSL.field(
        BACKFILL_PREFERENCE,
        SQLDataType.VARCHAR
          .asEnumDataType(
            BackfillPreference::class.java,
          ).nullable(false)
          .defaultValue(BackfillPreference.disabled),
      )

    private fun addBackfillTypeEnum(ctx: DSLContext) {
      ctx.createType(BACKFILL_PREFERENCE).asEnum(ENABLED, DISABLED).execute()
    }

    private fun addBackfillPreferenceColumnToSchemaManagementTable(ctx: DSLContext) {
      ctx
        .alterTable(SCHEMA_MANAGEMENT_TABLE)
        .addIfNotExists(BACKFILL_PREFERENCE_COLUMN)
        .execute()
    }
  }
}
