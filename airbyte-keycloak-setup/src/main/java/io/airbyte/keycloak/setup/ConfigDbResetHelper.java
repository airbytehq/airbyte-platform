/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.db.Database;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * Helper to reset the Config DB state as part of a Keycloak Realm Reset. Cleans up old User records
 * that would otherwise become orphaned when the Keycloak Realm is recreated from scratch, and
 * assigns new Keycloak auth IDs to SSO logins.
 */
@Singleton
public class ConfigDbResetHelper {

  private final Database configDb;

  public ConfigDbResetHelper(@Named("configDatabase") final Database configDb) {
    this.configDb = configDb;
  }

  public void deleteConfigDbUsers() throws SQLException {
    // DO NOT REMOVE THIS CRITICAL CHECK.
    throwIfMultipleOrganizations();

    this.configDb.transaction(ctx -> {
      final List<UUID> userIds = ctx.select(Tables.AUTH_USER.USER_ID)
          .from(Tables.AUTH_USER)
          .where(Tables.AUTH_USER.AUTH_PROVIDER.eq(AuthProvider.keycloak))
          .fetch(Tables.AUTH_USER.USER_ID);
      ctx.deleteFrom(Tables.USER)
          .where(Tables.USER.ID.in(userIds))
          .execute();
      return null;
    });
  }

  /**
   * This reset operation would be detrimental if it runs in any sort of multi-organization instance.
   * It relies on an assumption that all keycloak-backed users are part of the same
   * organization/realm. If the one-and-only realm is reset, we know the users will be orphaned. This
   * check is an extra layer of protection in case this code were somehow run in a multi-org
   * environment like Airbyte Cloud or any future multi-org setup.
   */
  private void throwIfMultipleOrganizations() throws SQLException {
    final var orgCount = this.configDb.query(ctx -> ctx.fetchCount(Tables.ORGANIZATION));
    int orgLimit = 1;
    if (orgCount > orgLimit) {
      throw new IllegalStateException("Multiple organizations found in ConfigDb. "
          + "This is not supported with the KEYCLOAK_RESET_REALM process.");
    }
  }

}
