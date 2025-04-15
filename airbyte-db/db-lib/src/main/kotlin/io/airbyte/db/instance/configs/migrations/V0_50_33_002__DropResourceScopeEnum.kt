/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

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
@Suppress("ktlint:standard:class-naming")
class V0_50_33_002__DropResourceScopeEnum : BaseJavaMigration() {
  // the commented code below was present in the original version of this migration.
  // private static final String RESOURCE_SCOPE = "resource_scope";

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

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
