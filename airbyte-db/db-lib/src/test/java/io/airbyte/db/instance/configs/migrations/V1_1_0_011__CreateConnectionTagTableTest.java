/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_0_011__CreateConnectionTagTableTest extends AbstractConfigsDatabaseTest {

  private static final String CONNECTION_TAG_TABLE = "connection_tag";
  private static final String ID = "id";
  private static final String TAG_ID = "tag_id";
  private static final String CONNECTION_ID = "connection_id";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_0_011__CreateConnectionTagTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V1_1_0_010__CreateTagTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
    final DSLContext ctx = getDslContext();
    V1_1_0_011__CreateConnectionTagTable.migrate(ctx);
  }

  @AfterEach
  void teardown() {
    // Fully tear down db after each test
    final DSLContext dslContext = getDslContext();
    dslContext.dropSchemaIfExists("public").cascade().execute();
    dslContext.createSchema("public").execute();
    dslContext.setSchema("public").execute();
  }

  @Test
  void testInsertConnectionTag() {
    final DSLContext ctx = getDslContext();
    final UUID tagId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    // Inserting a connection_tag relationship should succeed
    Assertions.assertDoesNotThrow(() -> {
      ctx.insertInto(DSL.table(CONNECTION_TAG_TABLE))
          .columns(
              DSL.field(ID),
              DSL.field(TAG_ID),
              DSL.field(CONNECTION_ID))
          .values(
              UUID.randomUUID(),
              tagId,
              connectionId)
          .execute();
    });
  }

  @Test
  void testUniqueTagConnectionConstraint() {
    final DSLContext ctx = getDslContext();
    final UUID tagId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    // Adding the first connection_tag relationship should succeed
    Assertions.assertDoesNotThrow(() -> {
      ctx.insertInto(DSL.table(CONNECTION_TAG_TABLE))
          .columns(
              DSL.field(ID),
              DSL.field(TAG_ID),
              DSL.field(CONNECTION_ID))
          .values(
              UUID.randomUUID(),
              tagId,
              connectionId)
          .execute();
    });

    // But adding a duplicate connection_tag relationship should fail
    final Exception e = Assertions.assertThrows(IntegrityConstraintViolationException.class, () -> {
      ctx.insertInto(DSL.table(CONNECTION_TAG_TABLE))
          .columns(
              DSL.field(ID),
              DSL.field(TAG_ID),
              DSL.field(CONNECTION_ID))
          .values(
              UUID.randomUUID(),
              tagId,
              connectionId)
          .execute();
    });
    Assertions
        .assertTrue(e.getMessage().contains("ERROR: duplicate key value violates unique constraint \"connection_tag_tag_id_connection_id_key\""));

  }

}
