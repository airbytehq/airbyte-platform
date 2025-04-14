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

@Suppress("ktlint:standard:class-naming")
class V0_57_4_009__RemoveOrgEmailUniqueConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    removeUniqueEmailDomainConstraint(ctx)
    addUniqueOrgIdAndDomainPairConstraint(ctx)
  }

  companion object {
    private fun removeUniqueEmailDomainConstraint(ctx: DSLContext) {
      ctx
        .alterTable("organization_email_domain")
        .dropConstraintIfExists("organization_email_domain_email_domain_key")
        .execute()
    }

    private fun addUniqueOrgIdAndDomainPairConstraint(ctx: DSLContext) {
      ctx
        .alterTable("organization_email_domain")
        .add(
          DSL
            .constraint("organization_id_email_domain_key")
            .unique(DSL.field("organization_id"), DSL.field("email_domain")),
        ).execute()
    }
  }
}
