/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Migration to add a uniqueness constraint on client_id in the dataplane_client_credentials table.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_1_012__AddUniquenessConstraintToDataplaneClientCredentials : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addUniqueConstraintOnClientId(ctx)
  }

  companion object {
    private const val DATAPLANE_CLIENT_CREDENTIALS_TABLE = "dataplane_client_credentials"

    private fun addUniqueConstraintOnClientId(ctx: DSLContext) {
      ctx
        .alterTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .add(DSL.unique("client_id"))
        .execute()
    }
  }
}
