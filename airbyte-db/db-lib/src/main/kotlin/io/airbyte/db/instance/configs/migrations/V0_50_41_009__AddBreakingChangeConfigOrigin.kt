/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_41_009__AddBreakingChangeConfigOrigin : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.alterType(CONFIG_ORIGIN_TYPE).addValue(BREAKING_CHANGE).execute()
  }

  internal enum class ConfigOriginType(
    private val literal: String,
  ) : EnumType {
    USER("user"),
    BREAKING_CHANGE("breaking_change"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONFIG_ORIGIN_TYPE

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val CONFIG_ORIGIN_TYPE = "config_origin_type"
    private const val BREAKING_CHANGE = "breaking_change"
  }
}
