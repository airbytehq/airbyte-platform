/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
class V0_50_24_007__AddUniqueConstraintInUserTableTest extends AbstractConfigsDatabaseTest {

  static final Table<?> USER_TABLE = DSL.table("\"user\"");
  static final Field<String> AUTH_USER_ID_FIELD = DSL.field("auth_user_id", String.class);
  static final Field<AuthProvider> AUTH_PROVIDER_FIELD = DSL.field("auth_provider", AuthProvider.class);

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_24_007__AddUniqueConstraintInUserTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static void insertUser(final DSLContext ctx, final String authUserId, final AuthProvider authProvider) {
    ctx.insertInto(USER_TABLE)
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("email"),
            AUTH_USER_ID_FIELD,
            AUTH_PROVIDER_FIELD)
        .values(
            UUID.randomUUID(),
            "user name",
            "user@email.com",
            authUserId,
            authProvider)
        .execute();
  }

  private static void assertUserCount(final DSLContext ctx, final String authUserId, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(USER_TABLE)
        .where(AUTH_USER_ID_FIELD.eq(authUserId))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  private static void assertUserTagCount(final DSLContext ctx, final String authUserId, final AuthProvider authProvider, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(USER_TABLE)
        .where(AUTH_USER_ID_FIELD.eq(authUserId))
        .and(AUTH_PROVIDER_FIELD.eq(authProvider))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  @Test
  void testMigrate() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final String authUserId1 = "authUserId1";
    final String authUserId2 = "authUserId2";
    final String authUserId3 = "authUserId3";

    final AuthProvider authProvider1 = AuthProvider.GOOGLE_IDENTITY_PLATFORM;
    final AuthProvider authProvider2 = AuthProvider.AIRBYTE;
    final AuthProvider authProvider3 = AuthProvider.KEYCLOAK;

    // Set up a state with multiple versions and some duplicates
    insertUser(ctx, authUserId1, authProvider1);
    insertUser(ctx, authUserId1, authProvider1);
    insertUser(ctx, authUserId1, authProvider2);

    insertUser(ctx, authUserId2, authProvider1);
    insertUser(ctx, authUserId2, authProvider2);
    insertUser(ctx, authUserId2, authProvider3);

    insertUser(ctx, authUserId3, authProvider1);
    insertUser(ctx, authUserId3, authProvider2);
    insertUser(ctx, authUserId3, authProvider3);
    insertUser(ctx, authUserId3, authProvider3);

    // Initial assertions
    assertUserCount(ctx, authUserId1, 3);
    assertUserCount(ctx, authUserId2, 3);
    assertUserCount(ctx, authUserId3, 4);

    assertUserTagCount(ctx, authUserId1, authProvider1, 2);
    assertUserTagCount(ctx, authUserId1, authProvider2, 1);
    assertUserTagCount(ctx, authUserId2, authProvider1, 1);
    assertUserTagCount(ctx, authUserId2, authProvider2, 1);
    assertUserTagCount(ctx, authUserId2, authProvider3, 1);
    assertUserTagCount(ctx, authUserId3, authProvider1, 1);
    assertUserTagCount(ctx, authUserId3, authProvider2, 1);
    assertUserTagCount(ctx, authUserId3, authProvider3, 2);

    // Run migration
    V0_50_24_007__AddUniqueConstraintInUserTable.migrate(ctx);

    // Assert duplicate rows were dropped
    assertUserCount(ctx, authUserId1, 2);
    assertUserCount(ctx, authUserId2, 3);
    assertUserCount(ctx, authUserId3, 3);

    assertUserTagCount(ctx, authUserId1, authProvider1, 1);
    assertUserTagCount(ctx, authUserId1, authProvider2, 1);
    assertUserTagCount(ctx, authUserId2, authProvider1, 1);
    assertUserTagCount(ctx, authUserId2, authProvider2, 1);
    assertUserTagCount(ctx, authUserId2, authProvider3, 1);
    assertUserTagCount(ctx, authUserId3, authProvider1, 1);
    assertUserTagCount(ctx, authUserId3, authProvider2, 1);
    assertUserTagCount(ctx, authUserId3, authProvider3, 1);

    // Attempting to re-insert an existing row should now fail
    assertThrows(DataAccessException.class, () -> insertUser(ctx, authUserId1, authProvider1));
  }

}
