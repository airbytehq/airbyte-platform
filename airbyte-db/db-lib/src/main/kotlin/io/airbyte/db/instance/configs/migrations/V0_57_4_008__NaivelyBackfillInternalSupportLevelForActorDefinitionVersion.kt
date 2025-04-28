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
 * This is a migration to naively populate all actor_definition_version records with a
 * internal_support_level relative to their release stage. general_availability -> 300 and the rest
 * is the default value `100`
 */
@Suppress("ktlint:standard:class-naming")
class V0_57_4_008__NaivelyBackfillInternalSupportLevelForActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    backfillInternalSupportLevel(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class ReleaseStage(
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
    fun backfillInternalSupportLevel(ctx: DSLContext) {
      ctx.transaction { configuration: Configuration? ->
        val transactionCtx = DSL.using(configuration)
        val updateQuery = "UPDATE actor_definition_version SET internal_support_level = {0} WHERE release_stage = {1}"

        // The default value set in the previous migration is `100L` which maps to `community`. To keep the
        // previous behavior which is "we alert on `connector_release_stage == generally_available`", we
        // will simply migrate these to a support level of `300L`. On new version releases, these
        // value will eventually align properly with the actual values in the metadata files.
        transactionCtx.execute(
          updateQuery,
          300L,
          ReleaseStage.generally_available,
        )

        // Drop the default Internal Support Level
        transactionCtx
          .alterTable("actor_definition_version")
          .alterColumn("internal_support_level")
          .dropDefault()
          .execute()
      }
    }
  }
}
