/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.Configs.AirbyteEdition
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_016__AddDataplaneGroupIdToConnection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    private val CONNECTION = DSL.table("connection")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
    private val DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
    private val CONNECTION_DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
    private val DATAPLANE_GROUP_PK = DSL.field("dataplane_group.id", SQLDataType.UUID.nullable(false))
    private val CONNECTION_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR.nullable(false))
    private val DATAPLANE_GROUP_NAME = DSL.field("dataplane_group.name", SQLDataType.VARCHAR.nullable(false))

    @JvmStatic
    fun doMigration(ctx: DSLContext) {
      addColumn(ctx)
      populateDataplaneGroupIds(ctx)
      addNotNullConstraint(ctx)
    }

    fun addColumn(ctx: DSLContext) {
      log.info { "Adding column dataplane_group_id to connection table" }
      ctx
        .alterTable(CONNECTION)
        .addColumnIfNotExists(DATAPLANE_GROUP_ID, SQLDataType.UUID.nullable(true))
        .execute()
    }

    private fun fromEnv(): AirbyteEdition? =
      when (System.getenv("AIRBYTE_EDITION")?.uppercase()) {
        "CLOUD" -> AirbyteEdition.CLOUD
        "COMMUNITY" -> AirbyteEdition.COMMUNITY
        "ENTERPRISE" -> AirbyteEdition.ENTERPRISE
        else -> null
      }

    fun populateDataplaneGroupIds(ctx: DSLContext) {
      log.info { "Updating connections with dataplane_group_id" }
      // Update connection table with corresponding dataplane_group_id
      when (fromEnv()) {
        AirbyteEdition.CLOUD -> {
          ctx
            .update(CONNECTION)
            .set(CONNECTION_DATAPLANE_GROUP_ID, DATAPLANE_GROUP_PK)
            .from(DATAPLANE_GROUP)
            .where(CONNECTION_GEOGRAPHY.cast(SQLDataType.VARCHAR).eq(DATAPLANE_GROUP_NAME))
            .execute()
        }
        else -> {
          // Only one dataplane group exists in non-cloud deploys that are running this migration,
          // so we don't have to match on `CONNECTION_GEOGRAPHY`
          ctx
            .update(CONNECTION)
            .set(CONNECTION_DATAPLANE_GROUP_ID, DATAPLANE_GROUP_PK)
            .from(DATAPLANE_GROUP)
            .execute()
        }
      }
    }

    fun addNotNullConstraint(ctx: DSLContext) {
      log.info { "Adding NOT NULL constraint to dataplane_group_id" }
      ctx
        .alterTable(CONNECTION)
        .alterColumn(DATAPLANE_GROUP_ID)
        .setNotNull()
        .execute()
    }
  }
}
