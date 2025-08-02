/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.jooq.JSONB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.UUID

internal class WorkspaceFilterTest : BaseConfigDatabaseTest() {
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  @Throws(IOException::class)
  fun beforeEach() {
    val featureFlagClient: FeatureFlagClient = Mockito.mock<TestClient>(TestClient::class.java)
    val secretsRepositoryReader = Mockito.mock<SecretsRepositoryReader>(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock<SecretsRepositoryWriter>(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)

    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )
  }

  @Test
  @DisplayName("Should return a list of active workspace IDs with most recently running jobs")
  @Throws(IOException::class)
  fun testListActiveWorkspacesByMostRecentlyRunningJobs() {
    val timeWindowInHours = 48
        /*
         * Following function is to filter workspace (IDs) with most recently running jobs within a given
         * time window. Step 1: Filter on table JOBS where job's UPDATED_AT timestamp is within the given
         * time window. Step 2: Trace back via CONNECTION table and ACTOR table. Step 3: Return workspace
         * IDs from ACTOR table.
         */
    val actualResult = workspaceService.listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours)
        /*
         * With the test data provided above, expected outputs for each step: Step 1: `jobs` (IDs) OL, 1L,
         * 2L, 3L, 4L, 5L and 6L. Step 2: `connections` (IDs) CONN_ID_0, CONN_ID_1, CONN_ID_2, CONN_ID_3,
         * and CONN_ID_4 `actors` (IDs) ACTOR_ID_0, ACTOR_ID_1, and ACTOR_ID_2. Step 3: `workspaces` (IDs)
         * WORKSPACE_ID_0, WORKSPACE_ID_1. Note that WORKSPACE_ID_2 is excluded because it is tombstoned.
         */
    val expectedResult: MutableList<UUID?> = ArrayList()
    expectedResult.add(WORKSPACE_ID_0)
    expectedResult.add(WORKSPACE_ID_1)
    Assertions.assertTrue(
      expectedResult.size == actualResult.size &&
        expectedResult.containsAll(actualResult) &&
        actualResult.containsAll(
          expectedResult,
        ),
    )
  }

  companion object {
    private val SRC_DEF_ID: UUID = UUID.randomUUID()
    private val DST_DEF_ID: UUID = UUID.randomUUID()
    private val SRC_DEF_VER_ID: UUID = UUID.randomUUID()
    private val DST_DEF_VER_ID: UUID = UUID.randomUUID()
    private val ACTOR_ID_0: UUID = UUID.randomUUID()
    private val ACTOR_ID_1: UUID = UUID.randomUUID()
    private val ACTOR_ID_2: UUID = UUID.randomUUID()
    private val ACTOR_ID_3: UUID = UUID.randomUUID()
    private val CONN_ID_0: UUID = UUID.randomUUID()
    private val CONN_ID_1: UUID = UUID.randomUUID()
    private val CONN_ID_2: UUID = UUID.randomUUID()
    private val CONN_ID_3: UUID = UUID.randomUUID()
    private val CONN_ID_4: UUID = UUID.randomUUID()
    private val CONN_ID_5: UUID = UUID.randomUUID()
    private val WORKSPACE_ID_0: UUID = UUID.randomUUID()
    private val WORKSPACE_ID_1: UUID = UUID.randomUUID()
    private val WORKSPACE_ID_2: UUID = UUID.randomUUID()
    private val WORKSPACE_ID_3: UUID = UUID.randomUUID()
    private val DEFAULT_DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()

    @BeforeAll
    @JvmStatic
    @Throws(SQLException::class, IOException::class)
    fun setUpAll() {
      // create organization first
      val defaultOrg = MockData.defaultOrganization()
      OrganizationServiceJooqImpl(database!!).writeOrganization(defaultOrg)
      val orgId = defaultOrg.organizationId

      // create dataplane group
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.DATAPLANE_GROUP,
              Tables.DATAPLANE_GROUP.ID,
              Tables.DATAPLANE_GROUP.ORGANIZATION_ID,
              Tables.DATAPLANE_GROUP.NAME,
              Tables.DATAPLANE_GROUP.ENABLED,
              Tables.DATAPLANE_GROUP.TOMBSTONE,
            ).values(DEFAULT_DATAPLANE_GROUP_ID, orgId, "default-dataplane", true, false)
            .execute()
        },
      )

      // create actor_definition
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.ACTOR_DEFINITION,
              Tables.ACTOR_DEFINITION.ID,
              Tables.ACTOR_DEFINITION.NAME,
              Tables.ACTOR_DEFINITION.ACTOR_TYPE,
            ).values(SRC_DEF_ID, "srcDef", ActorType.source)
            .values(DST_DEF_ID, "dstDef", ActorType.destination)
            .values(UUID.randomUUID(), "dstDef", ActorType.destination)
            .execute()
        },
      )
      // create actor_definition_version
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.ACTOR_DEFINITION_VERSION,
              Tables.ACTOR_DEFINITION_VERSION.ID,
              Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID,
              Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY,
              Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG,
              Tables.ACTOR_DEFINITION_VERSION.SPEC,
              Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL,
            ).values(SRC_DEF_VER_ID, SRC_DEF_ID, "airbyte/source", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
            .values(DST_DEF_VER_ID, DST_DEF_ID, "airbyte/destination", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
            .execute()
        },
      )

      // create workspace
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.WORKSPACE,
              Tables.WORKSPACE.ID,
              Tables.WORKSPACE.NAME,
              Tables.WORKSPACE.SLUG,
              Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
              Tables.WORKSPACE.TOMBSTONE,
              Tables.WORKSPACE.ORGANIZATION_ID,
              Tables.WORKSPACE.DATAPLANE_GROUP_ID,
            ).values(WORKSPACE_ID_0, "ws-0", "ws-0", true, false, orgId, DEFAULT_DATAPLANE_GROUP_ID)
            .values(
              WORKSPACE_ID_1,
              "ws-1",
              "ws-1",
              true,
              false,
              orgId,
              DEFAULT_DATAPLANE_GROUP_ID,
            ) // note that workspace 2 is tombstoned!
            .values(
              WORKSPACE_ID_2,
              "ws-2",
              "ws-2",
              true,
              true,
              orgId,
              DEFAULT_DATAPLANE_GROUP_ID,
            ) // note that workspace 3 is tombstoned!
            .values(WORKSPACE_ID_3, "ws-3", "ws-3", true, true, orgId, DEFAULT_DATAPLANE_GROUP_ID)
            .execute()
        },
      )
      // create actors
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.ACTOR,
              Tables.ACTOR.WORKSPACE_ID,
              Tables.ACTOR.ID,
              Tables.ACTOR.ACTOR_DEFINITION_ID,
              Tables.ACTOR.NAME,
              Tables.ACTOR.CONFIGURATION,
              Tables.ACTOR.ACTOR_TYPE,
            ).values(WORKSPACE_ID_0, ACTOR_ID_0, SRC_DEF_ID, "ACTOR-0", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_1, ACTOR_ID_1, SRC_DEF_ID, "ACTOR-1", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_2, ACTOR_ID_2, DST_DEF_ID, "ACTOR-2", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_3, ACTOR_ID_3, DST_DEF_ID, "ACTOR-3", JSONB.valueOf("{}"), ActorType.source)
            .execute()
        },
      )
      // create connections
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              Tables.CONNECTION,
              Tables.CONNECTION.SOURCE_ID,
              Tables.CONNECTION.DESTINATION_ID,
              Tables.CONNECTION.ID,
              Tables.CONNECTION.NAMESPACE_DEFINITION,
              Tables.CONNECTION.NAME,
              Tables.CONNECTION.CATALOG,
              Tables.CONNECTION.MANUAL,
            ).values(ACTOR_ID_0, ACTOR_ID_1, CONN_ID_0, NamespaceDefinitionType.source, "CONN-0", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_0, ACTOR_ID_2, CONN_ID_1, NamespaceDefinitionType.source, "CONN-1", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_1, ACTOR_ID_2, CONN_ID_2, NamespaceDefinitionType.source, "CONN-2", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_1, ACTOR_ID_2, CONN_ID_3, NamespaceDefinitionType.source, "CONN-3", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_2, ACTOR_ID_3, CONN_ID_4, NamespaceDefinitionType.source, "CONN-4", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_3, ACTOR_ID_1, CONN_ID_5, NamespaceDefinitionType.source, "CONN-5", JSONB.valueOf("{}"), true)
            .execute()
        },
      )
      // create jobs
      val currentTs = OffsetDateTime.now()
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .insertInto(
              io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS,
              io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID,
              io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT,
              io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE,
            ).values(0L, currentTs.minusHours(0), CONN_ID_0.toString())
            .values(1L, currentTs.minusHours(5), CONN_ID_0.toString())
            .values(2L, currentTs.minusHours(10), CONN_ID_1.toString())
            .values(3L, currentTs.minusHours(15), CONN_ID_1.toString())
            .values(4L, currentTs.minusHours(20), CONN_ID_2.toString())
            .values(5L, currentTs.minusHours(30), CONN_ID_3.toString())
            .values(6L, currentTs.minusHours(40), CONN_ID_4.toString())
            .values(7L, currentTs.minusHours(50), CONN_ID_4.toString())
            .values(8L, currentTs.minusHours(70), CONN_ID_5.toString())
            .execute()
        },
      )
    }
  }
}
