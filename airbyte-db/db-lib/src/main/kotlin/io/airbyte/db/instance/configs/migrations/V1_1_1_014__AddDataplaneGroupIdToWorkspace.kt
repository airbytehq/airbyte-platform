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
class V1_1_1_014__AddDataplaneGroupIdToWorkspace : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    private val WORKSPACE = DSL.table("workspace")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
    private val GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR)
    private val DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
    private val WORKSPACE_DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
    private val DATAPLANE_GROUP_PK = DSL.field("dataplane_group.id", SQLDataType.UUID.nullable(false))
    private val WORKSPACE_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR.nullable(false))
    private val DATAPLANE_GROUP_NAME = DSL.field("dataplane_group.name", SQLDataType.VARCHAR.nullable(false))

    @JvmStatic
    @VisibleForTesting
    fun doMigration(ctx: DSLContext) {
      addColumn(ctx)
      populateDataplaneGroupIds(ctx)
      addNotNullConstraint(ctx)
      updateGeographyColumnDropNotNullConstraintAndRename(ctx)
    }

    @VisibleForTesting
    fun addColumn(ctx: DSLContext) {
      log.info { "Adding column dataplane_group_id to workspace table" }
      ctx
        .alterTable(WORKSPACE)
        .addColumnIfNotExists(DATAPLANE_GROUP_ID, SQLDataType.UUID.nullable(true))
        .execute()
    }

    @VisibleForTesting
    fun populateDataplaneGroupIds(ctx: DSLContext) {
      log.info { "Updating workspaces with dataplane_group_id" }
      // Update workspace table with corresponding dataplane_group_id
      ctx
        .update(WORKSPACE)
        .set(WORKSPACE_DATAPLANE_GROUP_ID, DATAPLANE_GROUP_PK)
        .from(DATAPLANE_GROUP)
        .where(WORKSPACE_GEOGRAPHY.cast(SQLDataType.VARCHAR).eq(DATAPLANE_GROUP_NAME))
        .execute()
    }

    @VisibleForTesting
    fun addNotNullConstraint(ctx: DSLContext) {
      log.info { "Adding NOT NULL constraint to dataplane_group_id" }
      ctx
        .alterTable(WORKSPACE)
        .alterColumn(DATAPLANE_GROUP_ID)
        .setNotNull()
        .execute()
    }

    @VisibleForTesting
    fun updateGeographyColumnDropNotNullConstraintAndRename(ctx: DSLContext) {
      log.info { "Dropping NOT NULL constraint on geography" }
      ctx
        .alterTable(WORKSPACE)
        .alterColumn(GEOGRAPHY)
        .dropNotNull()
        .execute()
      log.info { "Renaming geography column to geography_DO_NOT_USE in workspace table" }
      ctx
        .alterTable(WORKSPACE)
        .renameColumn(GEOGRAPHY)
        .to("geography_DO_NOT_USE")
        .execute()
    }
  }
}
