/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType;
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jooq.JSONB;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkspaceFilterTest extends BaseConfigDatabaseTest {

  private static final UUID SRC_DEF_ID = UUID.randomUUID();
  private static final UUID DST_DEF_ID = UUID.randomUUID();
  private static final UUID SRC_DEF_VER_ID = UUID.randomUUID();
  private static final UUID DST_DEF_VER_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID_0 = UUID.randomUUID();
  private static final UUID ACTOR_ID_1 = UUID.randomUUID();
  private static final UUID ACTOR_ID_2 = UUID.randomUUID();
  private static final UUID ACTOR_ID_3 = UUID.randomUUID();
  private static final UUID CONN_ID_0 = UUID.randomUUID();
  private static final UUID CONN_ID_1 = UUID.randomUUID();
  private static final UUID CONN_ID_2 = UUID.randomUUID();
  private static final UUID CONN_ID_3 = UUID.randomUUID();
  private static final UUID CONN_ID_4 = UUID.randomUUID();
  private static final UUID CONN_ID_5 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_0 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_1 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_2 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_3 = UUID.randomUUID();
  private static final UUID DEFAULT_DATAPLANE_GROUP_ID = UUID.randomUUID();
  private WorkspaceService workspaceService;

  @BeforeAll
  static void setUpAll() throws SQLException, IOException {
    // create actor_definition
    database.transaction(ctx -> ctx.insertInto(ACTOR_DEFINITION, ACTOR_DEFINITION.ID, ACTOR_DEFINITION.NAME, ACTOR_DEFINITION.ACTOR_TYPE)
        .values(SRC_DEF_ID, "srcDef", ActorType.source)
        .values(DST_DEF_ID, "dstDef", ActorType.destination)
        .values(UUID.randomUUID(), "dstDef", ActorType.destination)
        .execute());
    // create actor_definition_version
    database.transaction(ctx -> ctx.insertInto(ACTOR_DEFINITION_VERSION, ACTOR_DEFINITION_VERSION.ID, ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID,
        ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, ACTOR_DEFINITION_VERSION.SPEC,
        ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL, ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL)
        .values(SRC_DEF_VER_ID, SRC_DEF_ID, "airbyte/source", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
        .values(DST_DEF_VER_ID, DST_DEF_ID, "airbyte/destination", "tag", JSONB.valueOf("{}"), SupportLevel.community, 100L)
        .execute());

    // create workspace
    database.transaction(
        ctx -> ctx
            .insertInto(WORKSPACE, WORKSPACE.ID, WORKSPACE.NAME, WORKSPACE.SLUG, WORKSPACE.INITIAL_SETUP_COMPLETE, WORKSPACE.TOMBSTONE,
                WORKSPACE.ORGANIZATION_ID, WORKSPACE.DATAPLANE_GROUP_ID)
            .values(WORKSPACE_ID_0, "ws-0", "ws-0", true, false, DEFAULT_ORGANIZATION_ID, DEFAULT_DATAPLANE_GROUP_ID)
            .values(WORKSPACE_ID_1, "ws-1", "ws-1", true, false, DEFAULT_ORGANIZATION_ID, DEFAULT_DATAPLANE_GROUP_ID)
            // note that workspace 2 is tombstoned!
            .values(WORKSPACE_ID_2, "ws-2", "ws-2", true, true, DEFAULT_ORGANIZATION_ID, DEFAULT_DATAPLANE_GROUP_ID)
            // note that workspace 3 is tombstoned!
            .values(WORKSPACE_ID_3, "ws-3", "ws-3", true, true, DEFAULT_ORGANIZATION_ID, DEFAULT_DATAPLANE_GROUP_ID)
            .execute());
    // create actors
    database.transaction(
        ctx -> ctx
            .insertInto(ACTOR, ACTOR.WORKSPACE_ID, ACTOR.ID, ACTOR.ACTOR_DEFINITION_ID, ACTOR.NAME, ACTOR.CONFIGURATION,
                ACTOR.ACTOR_TYPE)
            .values(WORKSPACE_ID_0, ACTOR_ID_0, SRC_DEF_ID, "ACTOR-0", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_1, ACTOR_ID_1, SRC_DEF_ID, "ACTOR-1", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_2, ACTOR_ID_2, DST_DEF_ID, "ACTOR-2", JSONB.valueOf("{}"), ActorType.source)
            .values(WORKSPACE_ID_3, ACTOR_ID_3, DST_DEF_ID, "ACTOR-3", JSONB.valueOf("{}"), ActorType.source)
            .execute());
    // create connections
    database.transaction(
        ctx -> ctx.insertInto(CONNECTION, CONNECTION.SOURCE_ID, CONNECTION.DESTINATION_ID, CONNECTION.ID, CONNECTION.NAMESPACE_DEFINITION,
            CONNECTION.NAME, CONNECTION.CATALOG, CONNECTION.MANUAL)
            .values(ACTOR_ID_0, ACTOR_ID_1, CONN_ID_0, NamespaceDefinitionType.source, "CONN-0", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_0, ACTOR_ID_2, CONN_ID_1, NamespaceDefinitionType.source, "CONN-1", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_1, ACTOR_ID_2, CONN_ID_2, NamespaceDefinitionType.source, "CONN-2", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_1, ACTOR_ID_2, CONN_ID_3, NamespaceDefinitionType.source, "CONN-3", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_2, ACTOR_ID_3, CONN_ID_4, NamespaceDefinitionType.source, "CONN-4", JSONB.valueOf("{}"), true)
            .values(ACTOR_ID_3, ACTOR_ID_1, CONN_ID_5, NamespaceDefinitionType.source, "CONN-5", JSONB.valueOf("{}"), true)
            .execute());
    // create jobs
    final OffsetDateTime currentTs = OffsetDateTime.now();
    database.transaction(ctx -> ctx.insertInto(JOBS, JOBS.ID, JOBS.UPDATED_AT, JOBS.SCOPE)
        .values(0L, currentTs.minusHours(0), CONN_ID_0.toString())
        .values(1L, currentTs.minusHours(5), CONN_ID_0.toString())
        .values(2L, currentTs.minusHours(10), CONN_ID_1.toString())
        .values(3L, currentTs.minusHours(15), CONN_ID_1.toString())
        .values(4L, currentTs.minusHours(20), CONN_ID_2.toString())
        .values(5L, currentTs.minusHours(30), CONN_ID_3.toString())
        .values(6L, currentTs.minusHours(40), CONN_ID_4.toString())
        .values(7L, currentTs.minusHours(50), CONN_ID_4.toString())
        .values(8L, currentTs.minusHours(70), CONN_ID_5.toString())
        .execute());

    new OrganizationServiceJooqImpl(database).writeOrganization(MockData.defaultOrganization());
  }

  @BeforeEach
  void beforeEach() throws IOException {
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final MetricClient metricClient = mock(MetricClient.class);

    workspaceService = new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient);
  }

  @Test
  @DisplayName("Should return a list of active workspace IDs with most recently running jobs")
  void testListActiveWorkspacesByMostRecentlyRunningJobs() throws IOException {
    final int timeWindowInHours = 48;
    /*
     * Following function is to filter workspace (IDs) with most recently running jobs within a given
     * time window. Step 1: Filter on table JOBS where job's UPDATED_AT timestamp is within the given
     * time window. Step 2: Trace back via CONNECTION table and ACTOR table. Step 3: Return workspace
     * IDs from ACTOR table.
     */
    final List<UUID> actualResult = workspaceService.listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours);
    /*
     * With the test data provided above, expected outputs for each step: Step 1: `jobs` (IDs) OL, 1L,
     * 2L, 3L, 4L, 5L and 6L. Step 2: `connections` (IDs) CONN_ID_0, CONN_ID_1, CONN_ID_2, CONN_ID_3,
     * and CONN_ID_4 `actors` (IDs) ACTOR_ID_0, ACTOR_ID_1, and ACTOR_ID_2. Step 3: `workspaces` (IDs)
     * WORKSPACE_ID_0, WORKSPACE_ID_1. Note that WORKSPACE_ID_2 is excluded because it is tombstoned.
     */
    final List<UUID> expectedResult = new ArrayList<>();
    expectedResult.add(WORKSPACE_ID_0);
    expectedResult.add(WORKSPACE_ID_1);
    assertTrue(expectedResult.size() == actualResult.size() && expectedResult.containsAll(actualResult) && actualResult.containsAll(expectedResult));
  }

}
