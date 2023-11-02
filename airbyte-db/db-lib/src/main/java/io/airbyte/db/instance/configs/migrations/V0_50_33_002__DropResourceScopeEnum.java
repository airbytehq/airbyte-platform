/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This enum isn't going to be used after all, and is currently conflicting with a Keycloak enum of
 * the same name. Dropping it to unblock Keycloak deployments that share a database with Airbyte
 * (which is bad and will be fixed in the near future).
 */
public class V0_50_33_002__DropResourceScopeEnum extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_002__DropResourceScopeEnum.class);

  private static final String RESOURCE_SCOPE = "resource_scope";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    // drop the column from SecretPersistenceConfig
    ctx.alterTable("secret_persistence_config")
        .dropColumnIfExists("scope_type")
        .execute();

    // drop the enum type
    ctx.dropTypeIfExists(RESOURCE_SCOPE).execute();
  }

}
