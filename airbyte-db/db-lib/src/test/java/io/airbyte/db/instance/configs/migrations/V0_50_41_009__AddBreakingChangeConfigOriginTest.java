/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType;
import io.airbyte.db.instance.configs.migrations.V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_41_009__AddBreakingChangeConfigOriginTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_41_009__AddBreakingChangeConfigOriginTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_41_009__AddBreakingChangeConfigOrigin();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private static void insertConfigWithOriginType(
                                                 final DSLContext ctx,
                                                 final ConfigOriginType originType) {
    ctx.insertInto(DSL.table("scoped_configuration"))
        .columns(
            DSL.field("id"),
            DSL.field("key"),
            DSL.field("resource_type"),
            DSL.field("resource_id"),
            DSL.field("scope_type"),
            DSL.field("scope_id"),
            DSL.field("value"),
            DSL.field("origin_type"),
            DSL.field("origin"))
        .values(
            UUID.randomUUID(),
            "some_key",
            ConfigResourceType.ACTOR_DEFINITION,
            UUID.randomUUID(),
            ConfigScopeType.ACTOR,
            UUID.randomUUID(),
            "my_value",
            originType,
            "origin_ref")
        .execute();
  }

  @Test
  void testBreakingChangeOriginScopedConfig() {
    final DSLContext ctx = getDslContext();

    insertConfigWithOriginType(ctx, ConfigOriginType.BREAKING_CHANGE); // does not throw

    assertThrows(IllegalArgumentException.class, () -> insertConfigWithOriginType(ctx, ConfigOriginType.valueOf("foo")));
  }

}
