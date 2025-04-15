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
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_41_006__AlterSupportLevelAddArchived : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateSupportLevelEnum(ctx)
    log.info { "support_level enum updated" }
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class SupportLevel(
    private val literal: String,
  ) : EnumType {
    community("community"),
    certified("certified"),
    archived("archived"),
    none("none"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "support_level"

    override fun getLiteral(): String = literal
  }

  companion object {
    fun updateSupportLevelEnum(ctx: DSLContext) {
      ctx.alterType("support_level").addValue("archived").execute()
    }
  }
}
