/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This enum isn't going to be used after all, and is currently conflicting with a Keycloak enum of
 * the same name. Dropping it to unblock Keycloak deployments that share a database with Airbyte
 * (which is bad and will be fixed in the near future).
 *
 * Edit: This migration is being modified after the fact to account for an issue with certain
 * Enterprise installations that populated their database with a conflicting Keycloak data type of
 * the same. V0_50_24_008 and V0_50_33_002 are being modified to avoid introducing the conflicting
 * datatype in the first place. Note that this effectively makes this migration a no-op, but it's
 * being left in place to avoid breaking the migration chain.
 */
public class V0_50_33_002__DropResourceScopeEnum extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_002__DropResourceScopeEnum.class);

  // the commented code below was present in the original version of this migration.
  // private static final String RESOURCE_SCOPE = "resource_scope";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.

    // the commented code below was present in the original version of this migration.

    // final DSLContext ctx = DSL.using(context.getConnection());

    // we no longer drop this column because the updated version of V0_50_24_008 no
    // longer adds it in the first place.

    // ctx.alterTable("secret_persistence_config")
    // .dropColumnIfExists("scope_type")
    // .execute();

    // the commented code below was present in the original version of this migration.
    // we no longer drop this type because the updated version of V0_50_24_008 no
    // longer adds it in the first place.

    // ctx.dropTypeIfExists(RESOURCE_SCOPE).execute();
  }

}
