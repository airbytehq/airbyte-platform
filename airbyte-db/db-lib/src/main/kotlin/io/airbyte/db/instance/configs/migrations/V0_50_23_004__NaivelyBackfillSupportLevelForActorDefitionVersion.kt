/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

/**
 * This is a migration to naively populate all actor_definition_version records with a support_level
 * relative to their release stage. alpha -> community beta -> community general_availability ->
 * certified
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    backfillSupportLevel(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class SupportLevel(
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

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class ReleaseStage(
    private val literal: String,
  ) : EnumType {
    alpha("alpha"),
    beta("beta"),
    generally_available("generally_available"),
    custom("custom"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "release_stage"

    override fun getLiteral(): String = literal
  }

  companion object {
    @JvmStatic
    fun backfillSupportLevel(ctx: DSLContext) {
      ctx.transaction { configuration: Configuration? ->
        val transactionCtx = DSL.using(configuration)
        val updateQuery =
          "UPDATE actor_definition_version SET support_level = {0} WHERE release_stage = {1} AND support_level = 'none'::support_level"

        // For all connections with invalid catalog, update to valid catalog
        transactionCtx.execute(updateQuery, SupportLevel.community, ReleaseStage.alpha)
        transactionCtx.execute(updateQuery, SupportLevel.community, ReleaseStage.beta)
        transactionCtx.execute(updateQuery, SupportLevel.certified, ReleaseStage.generally_available)
        transactionCtx.execute(updateQuery, SupportLevel.none, ReleaseStage.custom)

        // Drop the default Support Level
        transactionCtx
          .alterTable("actor_definition_version")
          .alterColumn("support_level")
          .dropDefault()
          .execute()
      }
    }
  }
}
