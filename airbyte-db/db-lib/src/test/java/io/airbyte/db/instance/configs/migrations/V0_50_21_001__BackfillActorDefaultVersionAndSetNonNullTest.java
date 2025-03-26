/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.Objects;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_21_001__BackfillActorDefaultVersionAndSetNonNullTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> ACTOR = DSL.table("actor");
  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");
  private static final Table<Record> ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version");
  private static final Table<Record> WORKSPACE = DSL.table("workspace");

  private static final Field<UUID> ID_COL = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> DEFAULT_VERSION_ID_COL = DSL.field("default_version_id", SQLDataType.UUID);
  private static final Field<UUID> ACTOR_DEFINITION_ID_COL = DSL.field("actor_definition_id", SQLDataType.UUID);

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final UUID VERSION_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_21_001__BackfillActorDefaultVersionAndSetNonNullTest.java", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_20_001__MakeManualNullableForRemoval();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private UUID getDefaultVersionIdForActorId(final DSLContext ctx, final UUID actorId) {
    final var actor = ctx.select(DEFAULT_VERSION_ID_COL)
        .from(ACTOR)
        .where(ID_COL.eq(actorId))
        .fetchOne();

    if (Objects.isNull(actor)) {
      return null;
    }

    return actor.get(DEFAULT_VERSION_ID_COL);
  }

  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  static void insertDependencies(final DSLContext ctx) {
    ctx.insertInto(WORKSPACE)
        .columns(
            ID_COL,
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"))
        .values(
            WORKSPACE_ID,
            "name1",
            "default",
            true)
        .execute();

    ctx.insertInto(ACTOR_DEFINITION)
        .columns(
            ID_COL,
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            ACTOR_DEFINITION_ID,
            "source def name",
            ActorType.source)
        .execute();

    ctx.insertInto(ACTOR_DEFINITION_VERSION)
        .columns(ID_COL, ACTOR_DEFINITION_ID_COL, DSL.field("docker_repository"), DSL.field("docker_image_tag"), DSL.field("spec"))
        .values(VERSION_ID, ACTOR_DEFINITION_ID, "airbyte/some-source", "1.0.0", JSONB.valueOf("{}"))
        .execute();

    ctx.update(ACTOR_DEFINITION)
        .set(DEFAULT_VERSION_ID_COL, VERSION_ID)
        .where(ID_COL.eq(ACTOR_DEFINITION_ID))
        .execute();
  }

  @Test
  void testBackFillActorDefaultVersionId() {
    final DSLContext ctx = getDslContext();
    insertDependencies(ctx);

    ctx.insertInto(ACTOR)
        .columns(
            ID_COL,
            ACTOR_DEFINITION_ID_COL,
            DSL.field("workspace_id"),
            DSL.field("name"),
            DSL.field("configuration"),
            DSL.field("actor_type"))
        .values(
            ACTOR_ID,
            ACTOR_DEFINITION_ID,
            WORKSPACE_ID,
            "My Source",
            JSONB.valueOf("{}"),
            ActorType.source)
        .execute();

    assertNull(getDefaultVersionIdForActorId(ctx, ACTOR_ID));

    V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull.backfillActorDefaultVersionId(ctx);

    assertEquals(VERSION_ID, getDefaultVersionIdForActorId(ctx, ACTOR_ID));
  }

  @Test
  void testActorDefaultVersionIdIsNotNull() {
    final DSLContext context = getDslContext();

    V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull.setNonNull(context);

    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(ACTOR)
          .columns(
              ID_COL,
              ACTOR_DEFINITION_ID_COL,
              DSL.field("workspace_id"),
              DSL.field("name"),
              DSL.field("configuration"),
              DSL.field("actor_type"))
          .values(
              UUID.randomUUID(),
              UUID.randomUUID(),
              UUID.randomUUID(),
              "My Source",
              JSONB.valueOf("{}"),
              ActorType.source)
          .execute();
    });
    Assertions.assertTrue(e.getMessage().contains("null value in column \"default_version_id\" of relation \"actor\" violates not-null constraint"));
  }

}
