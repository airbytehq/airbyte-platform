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
 * Add release stage to actor definition migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createReleaseStageEnum(ctx)
    addReleaseStageColumn(ctx)
    addReleaseDateColumn(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class ReleaseStage(
    private val literal: String,
  ) : EnumType {
    alpha("alpha"),
    beta("beta"),
    generally_available("generally_available"),
    custom("custom"),
    ;

    override fun getCatalog(): Catalog? = if (schema == null) null else schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "release_stage"

    override fun getLiteral(): String = literal
  }

  companion object {
    @JvmStatic
    fun createReleaseStageEnum(ctx: DSLContext) {
      ctx.createType("release_stage").asEnum("alpha", "beta", "generally_available", "custom").execute()
    }

    @JvmStatic
    fun addReleaseStageColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(
          DSL.field(
            "release_stage",
            SQLDataType.VARCHAR
              .asEnumDataType(
                ReleaseStage::class.java,
              ).nullable(true),
          ),
        ).execute()
    }

    @JvmStatic
    fun addReleaseDateColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("release_date", SQLDataType.DATE.nullable(true)))
        .execute()
    }
  }
}
