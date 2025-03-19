/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_0_010__CreateTagTableTest extends AbstractConfigsDatabaseTest {

  private static final String TAG = "tag";
  private static final String ID = "id";
  private static final String WORKSPACE_ID = "workspace_id";
  private static final String NAME = "name";
  private static final String COLOR = "color";
  private static final UUID workspaceId = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_0_010__CreateTagTableTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V1_1_0_009__AddPausedReasonToConnectorRollout();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    final DSLContext context = getDslContext();
    V1_1_0_010__CreateTagTable.createTagTable(context);

    // Create a workspace to add the tags to
    context.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("organization_id"))
        .values(
            workspaceId,
            "default",
            "default",
            true,
            DEFAULT_ORGANIZATION_ID)
        .execute();
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
  void testCanInsertTag() {
    final DSLContext context = getDslContext();
    final UUID tagId = UUID.randomUUID();

    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(TAG))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(COLOR),
              DSL.field(WORKSPACE_ID))
          .values(
              tagId,
              "Some tag",
              "000000",
              workspaceId)
          .execute();
    });
  }

  @Test
  void testDuplicateTestNameConstraint() {
    final DSLContext context = getDslContext();
    final String tagName = "Some tag";

    final UUID firstTagId = UUID.randomUUID();
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(TAG))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(COLOR),
              DSL.field(WORKSPACE_ID))
          .values(
              firstTagId,
              tagName,
              "000000",
              workspaceId)
          .execute();
    });

    final UUID secondTagId = UUID.randomUUID();
    final Exception e = Assertions.assertThrows(IntegrityConstraintViolationException.class, () -> {
      context.insertInto(DSL.table(TAG))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(COLOR),
              DSL.field(WORKSPACE_ID))
          .values(
              secondTagId,
              tagName,
              "000000",
              workspaceId)
          .execute();
    });
    Assertions.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint \"tag_name_workspace_id_key\""));
  }

  @Test
  void testValidHexConstraint() {
    final DSLContext context = getDslContext();
    final UUID tagId = UUID.randomUUID();

    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(TAG))
          .columns(
              DSL.field(ID),
              DSL.field(NAME),
              DSL.field(COLOR),
              DSL.field(WORKSPACE_ID))
          .values(
              tagId,
              "Some tag",
              "NOTHEX",
              workspaceId)
          .execute();
    });
    Assertions.assertTrue(e.getMessage().contains("new row for relation \"tag\" violates check constraint \"valid_hex_color\""));
  }

}
