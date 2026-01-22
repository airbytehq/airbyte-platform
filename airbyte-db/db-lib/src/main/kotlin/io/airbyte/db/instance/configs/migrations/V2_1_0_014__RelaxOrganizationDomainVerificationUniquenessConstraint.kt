/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_014__RelaxOrganizationDomainVerificationUniquenessConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    dropOldUniqueConstraint(ctx)
    createPartialUniqueIndex(ctx)

    log.info { "Migration completed: ${javaClass.simpleName}" }
  }

  companion object {
    private const val TABLE_NAME = "organization_domain_verification"
    private const val CONSTRAINT_NAME = "organization_domain_verification_organization_id_domain_key"
    private const val ORGANIZATION_ID_COLUMN = "organization_id"
    private const val DOMAIN_COLUMN = "domain"
    private const val TOMBSTONE_COLUMN = "tombstone"

    fun dropOldUniqueConstraint(ctx: DSLContext) {
      ctx
        .alterTable(TABLE_NAME)
        .dropConstraintIfExists(CONSTRAINT_NAME)
        .execute()
    }

    fun createPartialUniqueIndex(ctx: DSLContext) {
      ctx
        .createUniqueIndex(CONSTRAINT_NAME)
        .on(TABLE_NAME, ORGANIZATION_ID_COLUMN, DOMAIN_COLUMN)
        .where(DSL.field(TOMBSTONE_COLUMN).eq(false))
        .execute()
    }
  }
}
