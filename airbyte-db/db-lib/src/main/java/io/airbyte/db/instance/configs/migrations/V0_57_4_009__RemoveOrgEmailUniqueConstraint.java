/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_009__RemoveOrgEmailUniqueConstraint extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_009__RemoveOrgEmailUniqueConstraint.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    removeUniqueEmailDomainConstraint(ctx);
    addUniqueOrgIdAndDomainPairConstraint(ctx);
  }

  private static void removeUniqueEmailDomainConstraint(final DSLContext ctx) {
    ctx.alterTable("organization_email_domain")
        .dropConstraintIfExists("organization_email_domain_email_domain_key")
        .execute();
  }

  private static void addUniqueOrgIdAndDomainPairConstraint(final DSLContext ctx) {
    ctx.alterTable("organization_email_domain")
        .add(DSL.constraint("organization_id_email_domain_key").unique(DSL.field("organization_id"), DSL.field("email_domain")))
        .execute();
  }

}
