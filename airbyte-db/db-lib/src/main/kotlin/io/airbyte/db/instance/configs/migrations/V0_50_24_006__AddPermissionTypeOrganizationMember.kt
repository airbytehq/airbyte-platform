/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add a new Organization role as "ORGANIZATION_MEMBER".
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_006__AddPermissionTypeOrganizationMember : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.alterType(PERMISSION_TYPE_ENUM_NAME).addValue(ORGANIZATION_MEMBER).execute()
  }

  companion object {
    private const val PERMISSION_TYPE_ENUM_NAME = "permission_type"
    private const val ORGANIZATION_MEMBER = "organization_member"
  }
}
