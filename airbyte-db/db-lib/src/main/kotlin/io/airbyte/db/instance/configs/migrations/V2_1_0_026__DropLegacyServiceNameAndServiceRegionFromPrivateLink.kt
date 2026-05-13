/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.PRIVATE_LINK_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_026__DropLegacyServiceNameAndServiceRegionFromPrivateLink : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    ctx
      .alterTable(PRIVATE_LINK_TABLE)
      .dropColumn("service_name")
      .execute()

    ctx
      .alterTable(PRIVATE_LINK_TABLE)
      .dropColumn("service_region")
      .execute()
  }
}
