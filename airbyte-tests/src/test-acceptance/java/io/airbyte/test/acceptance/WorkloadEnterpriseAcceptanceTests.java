/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.acceptance.AcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.KUBE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.TRUE;
import static io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.CheckConnectionRead.Status;
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody;
import io.airbyte.test.acceptance.AcceptanceTestsResources.SyncIds;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Tests for operations utilizing the workload api / launcher. As development continues on these
 * components, and we migrate more operations to them, we will run these tests in CI to catch
 * regressions.
 */
@Tags({@Tag("enterprise")})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class WorkloadEnterpriseAcceptanceTests {

  AcceptanceTestsResources testResources = new AcceptanceTestsResources();

  static final UUID RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID = UUID.fromString("3d2985a0-a412-45f4-9124-e15800b739be");
  static final UUID RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID = UUID.fromString("1bdcfb61-219b-4290-be4f-12f9ac5461be");
  static final UUID RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID = UUID.fromString("3851861d-ac0b-440c-bd60-408cf9e7fc0e");

  @BeforeEach
  void setup() throws Exception {
    testResources.init();
    testResources.setup();
  }

  @AfterEach
  void tearDown() {
    testResources.tearDown();
  }

  @AfterEach
  void end() {
    testResources.end();
  }

  final boolean ranWithWorkload(final UUID connectionId, final long jobId, final int attemptNumber) throws IOException {
    final var attempt = testResources.getTestHarness().getApiClient().getAttemptApi().getAttemptForJob(
        new GetAttemptStatsRequestBody(jobId, attemptNumber));
    final String creatingWorkloadLog = "Starting workload heartbeat";
    return attempt.getLogs().getEvents().stream().anyMatch(l -> l.getMessage().contains(creatingWorkloadLog));
  }

  @Test
  @EnabledIfEnvironmentVariable(named = KUBE,
                                matches = TRUE)
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testSyncWithWorkload() throws Exception {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an
    // override in order to exercise the workload path.
    testResources.getTestHarness().createWorkspaceWithId(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID);

    final SyncIds syncIds;
    syncIds = testResources.runSmallSyncForAWorkspaceId(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID);
    assertTrue(ranWithWorkload(syncIds.connectionId(), syncIds.jobId(), syncIds.attemptNumber()),
        "Failed for workspace:" + RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID + " connection:" + syncIds.connectionId() + " job:"
            + syncIds.jobId());
  }

  @Test
  @EnabledIfEnvironmentVariable(named = KUBE,
                                matches = TRUE)
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testDestinationCheckConnectionWithWorkload() throws Exception {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.getTestHarness().createWorkspaceWithId(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID);

    final UUID destinationId = testResources.getTestHarness().createPostgresDestination(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID).getDestinationId();

    final Status checkOperationStatus = testResources.getTestHarness().checkDestination(destinationId);

    Assertions.assertNotNull(checkOperationStatus);
    assertEquals(Status.SUCCEEDED, checkOperationStatus);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = KUBE,
                                matches = TRUE)
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testDiscoverSourceSchema() throws Exception {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.getTestHarness().createWorkspaceWithId(RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID);

    final UUID sourceId = testResources.getTestHarness().createPostgresSource(RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID).getSourceId();

    final AirbyteCatalog actual = testResources.getTestHarness().discoverSourceSchema(sourceId);

    testResources.getTestHarness().compareCatalog(actual);
  }

}
