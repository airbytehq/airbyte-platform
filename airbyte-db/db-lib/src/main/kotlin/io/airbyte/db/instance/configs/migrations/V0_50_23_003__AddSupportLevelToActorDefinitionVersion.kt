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
 * Inserts a support_level column to the actor_definition_version table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_23_003__AddSupportLevelToActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addSupportLevelToActorDefinitionVersion(ctx)
    log.info { "support_level column added to actor_definition_version table" }
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class SupportLevel(
    private val literal: String,
  ) : EnumType {
    community("community"),
    certified("certified"),
    none("none"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "support_level"

    override fun getLiteral(): String = literal
  }

  companion object {
    @JvmStatic
    fun addSupportLevelToActorDefinitionVersion(ctx: DSLContext) {
      createSupportLevelEnum(ctx)
      addSupportLevelColumn(ctx)
    }

    private fun createSupportLevelEnum(ctx: DSLContext) {
      ctx.createType("support_level").asEnum("community", "certified", "none").execute()
    }

    private fun addSupportLevelColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumnIfNotExists(
          DSL.field(
            "support_level",
            SQLDataType.VARCHAR
              .asEnumDataType(
                SupportLevel::class.java,
              ).nullable(false)
              .defaultValue(SupportLevel.none),
          ),
        ).execute()
    }
  }
}
