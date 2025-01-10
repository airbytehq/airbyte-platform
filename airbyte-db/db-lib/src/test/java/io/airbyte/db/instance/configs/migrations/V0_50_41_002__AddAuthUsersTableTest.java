/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.AUTH_USER_TABLE;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_41_002__AddAuthUsersTableTest extends AbstractConfigsDatabaseTest {

  private static final String EXPECTED_DEFAULT_USER_AUTH_USER_ID = "00000000-0000-0000-0000-000000000000";
  private static final AuthProvider EXPECTED_DEFAULT_USER_AUTH_PROVIDER = AuthProvider.AIRBYTE;

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_33_018__AddAuthUsersTable", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_33_016__AddIconUrlToActorDefinition();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testPopulateAuthUsersTable() throws SQLException, IOException {
    final DSLContext context = getDslContext();

    V0_50_41_002__AddAuthUsersTable.createAuthUsersTable(context);
    V0_50_41_002__AddAuthUsersTable.populateAuthUserTable(context);

    Assertions.assertTrue(authUserRowForDefaultUserExists(context));
  }

  static boolean authUserRowForDefaultUserExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from(AUTH_USER_TABLE)
        .where(DSL.field("auth_user_id", String.class).eq(EXPECTED_DEFAULT_USER_AUTH_USER_ID))
        .and(DSL.field("auth_provider", SQLDataType.VARCHAR.asEnumDataType(AuthProvider.class)).eq(EXPECTED_DEFAULT_USER_AUTH_PROVIDER)));
  }

}
