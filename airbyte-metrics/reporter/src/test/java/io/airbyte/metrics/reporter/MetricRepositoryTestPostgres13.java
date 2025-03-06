/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter;

import static io.airbyte.db.instance.configs.jooq.generated.Keys.ACTOR_CATALOG_FETCH_EVENT__ACTOR_CATALOG_FETCH_EVENT_ACTOR_ID_FKEY;
import static io.airbyte.db.instance.configs.jooq.generated.Keys.ACTOR__ACTOR_WORKSPACE_ID_FKEY;
import static io.airbyte.db.instance.configs.jooq.generated.Keys.CONNECTION__CONNECTION_DESTINATION_ID_FKEY;
import static io.airbyte.db.instance.configs.jooq.generated.Keys.CONNECTION__CONNECTION_SOURCE_ID_FKEY;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG_FETCH_EVENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;

import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.test.utils.Databases;
import java.io.IOException;
import java.util.UUID;
import org.jooq.JSONB;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

public class MetricRepositoryTestPostgres13 extends MetricRepositoryTest {

  @BeforeAll
  public static void setUpAll() throws DatabaseInitializationException, IOException {
    final var psqlContainer = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withUsername("user")
        .withPassword("hunter2");
    psqlContainer.start();

    final var dataSource = Databases.createDataSource(psqlContainer);
    ctx = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final var dbProviders = new TestDatabaseProviders(dataSource, ctx);
    dbProviders.createNewConfigsDatabase();
    dbProviders.createNewJobsDatabase();

    ctx.insertInto(ACTOR_DEFINITION, ACTOR_DEFINITION.ID, ACTOR_DEFINITION.NAME, ACTOR_DEFINITION.ACTOR_TYPE)
        .values(SRC_DEF_ID, "srcDef", ActorType.source)
        .values(DST_DEF_ID, "dstDef", ActorType.destination)
        .values(UUID.randomUUID(), "dstDef", ActorType.destination)
        .execute();

    ctx.insertInto(ACTOR_DEFINITION_VERSION, ACTOR_DEFINITION_VERSION.ID, ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID,
        ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, ACTOR_DEFINITION_VERSION.SPEC,
        ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL, ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL)
        .values(SRC_DEF_VER_ID, SRC_DEF_ID, "airbyte/source", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
        .values(DST_DEF_VER_ID, DST_DEF_ID, "airbyte/destination", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
        .execute();

    // drop constraints to simplify test set up
    ctx.alterTable(ACTOR).dropForeignKey(ACTOR__ACTOR_WORKSPACE_ID_FKEY.constraint()).execute();
    ctx.alterTable(CONNECTION).dropForeignKey(CONNECTION__CONNECTION_DESTINATION_ID_FKEY.constraint()).execute();
    ctx.alterTable(CONNECTION).dropForeignKey(CONNECTION__CONNECTION_SOURCE_ID_FKEY.constraint()).execute();
    ctx.alterTable(ACTOR_CATALOG_FETCH_EVENT)
        .dropForeignKey(ACTOR_CATALOG_FETCH_EVENT__ACTOR_CATALOG_FETCH_EVENT_ACTOR_ID_FKEY.constraint()).execute();
    ctx.alterTable(WORKSPACE).alter(WORKSPACE.SLUG).dropNotNull().execute();
    ctx.alterTable(WORKSPACE).alter(WORKSPACE.INITIAL_SETUP_COMPLETE).dropNotNull().execute();

    db = new MetricRepository(ctx);
  }

}
