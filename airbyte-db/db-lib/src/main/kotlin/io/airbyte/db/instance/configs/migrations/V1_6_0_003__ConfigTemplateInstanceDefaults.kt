/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_003__ConfigTemplateInstanceDefaults : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateConfigTemplateTable(ctx)
  }

  companion object {
    private const val TABLE_NAME = "config_template"

    // Column names
    private const val ID_COL = "id"
    private const val ACTOR_DEF_ID_COL = "actor_definition_id"
    private const val ACTOR_DEF_VERSION_ID_COL = "actor_definition_version_id"
    private const val ORG_ID_COL = "organization_id"

    // Table names
    private const val ACTOR_DEF_VERSION_TABLE = "actor_definition_version"

    // Constraint names
    private const val FK_ACTOR_DEF_VERSION_CONSTRAINT = "config_template_actor_definition_version_id_fkey"
    private const val UNIQUE_ACTOR_DEF_VERSION_CONSTRAINT = "config_template_actor_definition_version_id_unique"
    private const val VALID_REFERENCE_CHECK_CONSTRAINT = "config_template_valid_reference_check"

    @VisibleForTesting
    fun updateConfigTemplateTable(ctx: DSLContext) {
      log.info { "Updating $TABLE_NAME table" }

      // Add actor_definition_version_id column with FK + uniqueness constraint
      ctx
        .alterTable(TABLE_NAME)
        .addColumnIfNotExists(
          DSL.field(
            ACTOR_DEF_VERSION_ID_COL,
            SQLDataType.UUID.nullable(true),
          ),
        ).execute()

      ctx
        .alterTable(TABLE_NAME)
        .add(
          DSL
            .constraint(FK_ACTOR_DEF_VERSION_CONSTRAINT)
            .foreignKey(ACTOR_DEF_VERSION_ID_COL)
            .references(ACTOR_DEF_VERSION_TABLE, ID_COL),
        ).execute()

      ctx
        .alterTable(TABLE_NAME)
        .add(
          DSL
            .constraint(UNIQUE_ACTOR_DEF_VERSION_CONSTRAINT)
            .unique(ACTOR_DEF_VERSION_ID_COL),
        ).execute()

      // Drop non-null constraint from actor_definition_id
      ctx
        .alterTable(TABLE_NAME)
        .alterColumn(ACTOR_DEF_ID_COL)
        .dropNotNull()
        .execute()

      // Drop non-null constraint from organization_id
      ctx
        .alterTable(TABLE_NAME)
        .alterColumn(ORG_ID_COL)
        .dropNotNull()
        .execute()

      // Add check constraint for the required rule
      ctx
        .alterTable(TABLE_NAME)
        .add(
          DSL
            .constraint(VALID_REFERENCE_CHECK_CONSTRAINT)
            .check(
              DSL.or(
                DSL.and(
                  DSL.field(ORG_ID_COL).isNotNull,
                  DSL.field(ACTOR_DEF_ID_COL).isNotNull,
                  DSL.field(ACTOR_DEF_VERSION_ID_COL).isNull,
                ),
                DSL.and(
                  DSL.field(ORG_ID_COL).isNull,
                  DSL.field(ACTOR_DEF_ID_COL).isNull,
                  DSL.field(ACTOR_DEF_VERSION_ID_COL).isNotNull,
                ),
              ),
            ),
        ).execute()

      log.info { "Successfully updated $TABLE_NAME table" }
    }
  }
}
