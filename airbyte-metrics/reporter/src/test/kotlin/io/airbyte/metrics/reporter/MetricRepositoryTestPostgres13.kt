/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.test.utils.Databases
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.PostgreSQLContainer
import java.io.IOException
import java.util.UUID

object MetricRepositoryTestPostgres13 : MetricRepositoryTest() {
  @BeforeAll
  @Throws(DatabaseInitializationException::class, IOException::class)
  @JvmStatic
  fun setUpAll() {
    val psqlContainer =
      PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withUsername("user")
        .withPassword("hunter2")
    psqlContainer.start()

    val dataSource = Databases.createDataSource(psqlContainer)
    ctx = create(dataSource, SQLDialect.POSTGRES)
    val dbProviders = TestDatabaseProviders(dataSource, ctx!!)
    dbProviders.createNewConfigsDatabase()
    dbProviders.createNewJobsDatabase()

    ctx!!
      .insertInto(
        Tables.ACTOR_DEFINITION,
        Tables.ACTOR_DEFINITION.ID,
        Tables.ACTOR_DEFINITION.NAME,
        Tables.ACTOR_DEFINITION.ACTOR_TYPE,
      ).values(SRC_DEF_ID, "srcDef", ActorType.source)
      .values(DST_DEF_ID, "dstDef", ActorType.destination)
      .values(UUID.randomUUID(), "dstDef", ActorType.destination)
      .execute()

    ctx!!
      .insertInto(
        Tables.ACTOR_DEFINITION_VERSION,
        Tables.ACTOR_DEFINITION_VERSION.ID,
        Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID,
        Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY,
        Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG,
        Tables.ACTOR_DEFINITION_VERSION.SPEC,
        Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
        Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL,
      ).values(
        SRC_DEF_VER_ID,
        SRC_DEF_ID,
        "airbyte/source",
        "tag",
        JSONB.valueOf("{}"),
        SupportLevel.community,
        100L,
      ).values(
        DST_DEF_VER_ID,
        DST_DEF_ID,
        "airbyte/destination",
        "tag",
        JSONB.valueOf("{}"),
        SupportLevel.community,
        100L,
      ).execute()

    // drop constraints to simplify test set up
    ctx!!.alterTable(Tables.ACTOR).dropForeignKey(Keys.ACTOR__ACTOR_WORKSPACE_ID_FKEY.constraint()).execute()
    ctx!!
      .alterTable(Tables.CONNECTION)
      .dropForeignKey(Keys.CONNECTION__CONNECTION_DESTINATION_ID_FKEY.constraint())
      .execute()
    ctx!!
      .alterTable(Tables.CONNECTION)
      .dropForeignKey(Keys.CONNECTION__CONNECTION_SOURCE_ID_FKEY.constraint())
      .execute()
    ctx!!
      .alterTable(Tables.ACTOR_CATALOG_FETCH_EVENT)
      .dropForeignKey(Keys.ACTOR_CATALOG_FETCH_EVENT__ACTOR_CATALOG_FETCH_EVENT_ACTOR_ID_FKEY.constraint())
      .execute()
    ctx!!
      .alterTable(Tables.WORKSPACE)
      .alter(Tables.WORKSPACE.SLUG)
      .dropNotNull()
      .execute()
    ctx!!
      .alterTable(Tables.WORKSPACE)
      .alter(Tables.WORKSPACE.INITIAL_SETUP_COMPLETE)
      .dropNotNull()
      .execute()

    db = MetricRepository(ctx!!)
  }
}
