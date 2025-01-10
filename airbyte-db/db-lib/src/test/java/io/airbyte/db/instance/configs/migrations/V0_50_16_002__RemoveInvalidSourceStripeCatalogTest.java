/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_50_16_002__RemoveInvalidSourceStripeCatalogTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_16_002__RemoveInvalidSourceStripeCatalog();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  private UUID insertDummyActorCatalog(final DSLContext ctx, final String catalogHash) {
    final UUID actorCatalogId = UUID.randomUUID();
    ctx.insertInto(DSL.table("actor_catalog"))
        .columns(
            DSL.field("id"),
            DSL.field("catalog"),
            DSL.field("catalog_hash"),
            DSL.field("created_at"))
        .values(
            actorCatalogId,
            JSONB.valueOf("{}"),
            catalogHash,
            DSL.currentTimestamp())
        .execute();
    return actorCatalogId;
  }

  private UUID insertDummyConnection(final DSLContext ctx, final UUID sourceCatalogId) {
    final UUID connectionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();

    ctx.insertInto(DSL.table("connection"))
        .columns(
            DSL.field("id"),
            DSL.field("source_catalog_id"),
            DSL.field("source_id"),
            DSL.field("destination_id"),
            DSL.field("namespace_definition"),
            DSL.field("name"),
            DSL.field("catalog"),
            DSL.field("manual")

        )
        .values(
            connectionId,
            sourceCatalogId,
            sourceId,
            destinationId,
            DSL.field("cast(? as namespace_definition_type)", "customformat"),
            "dummyname",
            JSONB.valueOf("{}"),
            false)
        .execute();
    return connectionId;
  }

  @Test
  void testRemoveInvalidSourceStripeCatalog() throws Exception {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final UUID badActorCatalogId = insertDummyActorCatalog(ctx, V0_50_16_002__RemoveInvalidSourceStripeCatalog.INVALID_CATALOG_CONTENT_HASH);
    final UUID goodActorCatalogId = insertDummyActorCatalog(ctx, V0_50_16_002__RemoveInvalidSourceStripeCatalog.VALID_CATALOG_CONTENT_HASH);
    final UUID otherActorCatalogId = insertDummyActorCatalog(ctx, "other_hash");

    final int numberOfConnections = 10;

    final List<UUID> badConnectionIds = IntStream.range(0, numberOfConnections)
        .mapToObj(i -> insertDummyConnection(ctx, badActorCatalogId))
        .toList();

    final List<UUID> goodConnectionIds = IntStream.range(0, numberOfConnections)
        .mapToObj(i -> insertDummyConnection(ctx, goodActorCatalogId))
        .toList();

    final List<UUID> otherConnectionIds = IntStream.range(0, numberOfConnections)
        .mapToObj(i -> insertDummyConnection(ctx, otherActorCatalogId))
        .toList();

    V0_50_16_002__RemoveInvalidSourceStripeCatalog.removeInvalidSourceStripeCatalog(ctx);

    // check that the bad catalog is deleted
    final List<Record> badCatalogs = ctx.select()
        .from(DSL.table("actor_catalog"))
        .where(DSL.field("id").in(badActorCatalogId))
        .fetch();

    Assertions.assertEquals(0, badCatalogs.size());

    // check that the good catalog and other catalog is not deleted
    final List<Record> goodCatalogs = ctx.select()
        .from(DSL.table("actor_catalog"))
        .where(DSL.field("id").in(goodActorCatalogId).or(DSL.field("id").in(otherActorCatalogId)))
        .fetch();

    Assertions.assertEquals(2, goodCatalogs.size());

    // check that the previously bad connections and the good connections reference the good catalog
    // i.e. the bad connections now have a source_catalog_id that references the good catalog
    final List<Record> previouslyBadConnections = ctx.select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").in(badConnectionIds))
        .fetch();

    Assertions.assertEquals(numberOfConnections, previouslyBadConnections.size());
    Assertions.assertTrue(previouslyBadConnections.stream().allMatch(r -> r.get(DSL.field("source_catalog_id")).equals(goodActorCatalogId)));

    // check that the good connections still reference the good catalog
    final List<Record> goodConnections = ctx.select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").in(goodConnectionIds))
        .fetch();

    Assertions.assertEquals(numberOfConnections, goodConnections.size());
    Assertions.assertTrue(goodConnections.stream().allMatch(r -> r.get(DSL.field("source_catalog_id")).equals(goodActorCatalogId)));

    // check that the other connections still reference the other catalog
    final List<Record> otherConnections = ctx.select()
        .from(DSL.table("connection"))
        .where(DSL.field("id").in(otherConnectionIds))
        .fetch();

    Assertions.assertEquals(numberOfConnections, otherConnections.size());
    Assertions.assertTrue(otherConnections.stream().allMatch(r -> r.get(DSL.field("source_catalog_id")).equals(otherActorCatalogId)));
  }

}
