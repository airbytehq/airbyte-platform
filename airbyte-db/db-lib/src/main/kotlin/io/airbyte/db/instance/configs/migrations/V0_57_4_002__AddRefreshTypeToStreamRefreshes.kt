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
class V0_57_4_002__AddRefreshTypeToStreamRefreshes : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createRefreshTypeEnum(ctx)
    addRefreshTypeToStreamRefreshes(ctx)
    removeDefaultRefreshType(ctx)
  }

  private fun createRefreshTypeEnum(ctx: DSLContext) {
    ctx
      .createType(RefreshType.NAME)
      .asEnum(*RefreshType.entries.map { it.literal }.toTypedArray())
      .execute()
  }

  private fun addRefreshTypeToStreamRefreshes(ctx: DSLContext) {
    ctx
      .alterTable(STREAM_REFRESHES)
      .addColumnIfNotExists(
        DSL.field(
          RefreshType.NAME,
          SQLDataType.VARCHAR
            .asEnumDataType(
              RefreshType::class.java,
            ).defaultValue(RefreshType.TRUNCATE)
            .nullable(false),
        ),
      ).execute()
  }

  private fun removeDefaultRefreshType(ctx: DSLContext) {
    ctx
      .alterTable(STREAM_REFRESHES)
      .alterColumn(DSL.field(RefreshType.NAME))
      .dropDefault()
      .execute()
  }

  enum class RefreshType(
    private val literal: String,
  ) : EnumType {
    MERGE("MERGE"),
    TRUNCATE("TRUNCATE"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "refresh_type"
    }
  }

  companion object {
    private const val STREAM_REFRESHES = "stream_refreshes"
  }
}
