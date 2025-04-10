/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Some dataplanes and dataplane groups will be created by the system and/or non-person entities, so
 * a foreign key to the user table isn't sufficient to provide audit information. Dropping these
 * columns and will revisit the audit strategy later on.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_1_023__DropUserForeignKeysFromDataplaneTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropDataplaneUpdatedBy(ctx)
    dropDataplaneGroupUpdatedBy(ctx)
    dropDataplaneClientCredentialsCreatedBy(ctx)
  }

  companion object {
    private val DATAPLANE_TABLE = DSL.table("dataplane")
    private val DATAPLANE_GROUP_TABLE = DSL.table("dataplane_group")
    private val DATAPLANE_CLIENT_CREDENTIALS_TABLE = DSL.table("dataplane_client_credentials")
    private val UPDATED_BY = DSL.field("updated_by", SQLDataType.UUID)
    private val CREATED_BY = DSL.field("created_by", SQLDataType.UUID)

    fun dropDataplaneUpdatedBy(ctx: DSLContext) {
      log.info { "Dropping 'updated_by' column from Dataplane table" }
      ctx
        .alterTable(DATAPLANE_TABLE)
        .dropColumnIfExists(UPDATED_BY)
        .execute()
    }

    fun dropDataplaneGroupUpdatedBy(ctx: DSLContext) {
      log.info { "Dropping 'updated_by' column from DataplaneGroup table" }
      ctx
        .alterTable(DATAPLANE_GROUP_TABLE)
        .dropColumnIfExists(UPDATED_BY)
        .execute()
    }

    fun dropDataplaneClientCredentialsCreatedBy(ctx: DSLContext) {
      log.info { "Dropping 'created_by' column from DataplaneClientCredentials table" }
      ctx
        .alterTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .dropColumnIfExists(CREATED_BY)
        .execute()
    }
  }
}
