/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_1_002_AddEnterpriseToActorDefinitionTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_1_002_AddEnterpriseToActorDefinitionTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V1_1_1_001__AddResourceRequirementsToActor();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void test() throws SQLException, IOException {
    final DSLContext context = getDslContext();

    Assertions.assertFalse(enterpriseColumnExists(context));

    final UUID id = UUID.randomUUID();
    context.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            id,
            "name",
            ActorType.source)
        .execute();

    V1_1_1_002__AddEnterpriseToActorDefinition.addEnterpriseColumn(context);

    Assertions.assertTrue(enterpriseColumnExists(context));
    Assertions.assertTrue(enterpriseDefaultsToFalse(context, id));
  }

  protected static boolean enterpriseColumnExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq("actor_definition")
            .and(DSL.field("column_name").eq("enterprise"))));
  }

  protected static boolean enterpriseDefaultsToFalse(final DSLContext ctx, final UUID id) {
    final Record record = ctx.fetchOne(DSL.select()
        .from("actor_definition")
        .where(DSL.field("id").eq(id)));

    return record.get("enterprise").equals(false);
  }

}
