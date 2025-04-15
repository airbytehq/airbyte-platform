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
 * Adds support state to actor definition version. This migration does two things: 1. Adds a
 * support_state enum type. 2. Inserts a support_state column into the actor_definition_version
 * table. The support_state is a string that can be used to indicate whether an actor definition
 * version is supported, unsupported, or deprecated.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_7_001__AddSupportStateToActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addSupportStateType(ctx)
    addSupportStateColumnToActorDefinitionVersion(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class SupportState(
    private val literal: String,
  ) : EnumType {
    supported("supported"),
    deprecated("deprecated"),
    unsupported("unsupported"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = SUPPORT_STATE_TYPE_NAME

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val SUPPORT_STATE_TYPE_NAME = "support_state"

    @JvmStatic
    fun addSupportStateType(ctx: DSLContext) {
      ctx.dropTypeIfExists(SUPPORT_STATE_TYPE_NAME).execute()
      ctx.createType(SUPPORT_STATE_TYPE_NAME).asEnum("supported", "deprecated", "unsupported").execute()
    }

    @JvmStatic
    fun addSupportStateColumnToActorDefinitionVersion(ctx: DSLContext) {
      val supportState =
        DSL.field(
          "support_state",
          SQLDataType.VARCHAR
            .asEnumDataType(
              SupportState::class.java,
            ).nullable(false)
            .defaultValue(SupportState.supported),
        )
      ctx.alterTable("actor_definition_version").addColumnIfNotExists(supportState).execute()

      log.info { "Added support_state column to actor_definition_version table and set to 'supported' for all existing rows" }
    }
  }
}
