/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.acceptance.BasicAcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE;
import static io.airbyte.test.acceptance.BasicAcceptanceTestsResources.IS_GKE;
import static io.airbyte.test.acceptance.BasicAcceptanceTestsResources.KUBE;
import static io.airbyte.test.acceptance.BasicAcceptanceTestsResources.TRUE;

import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead.StatusEnum;
import io.airbyte.api.client.model.generated.WorkspaceCreateWithId;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for operations utilizing the workload api / launcher. As development continues on these
 * components, and we migrate more operations to them, we will run these tests in CI to catch
 * regressions.
 */
public class WorkloadBasicAcceptanceTests {

  static final Logger LOGGER = LoggerFactory.getLogger(WorkloadBasicAcceptanceTests.class);

  static final BasicAcceptanceTestsResources testResources = new BasicAcceptanceTestsResources();

  static final UUID RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID = UUID.fromString("3d2985a0-a412-45f4-9124-e15800b739be");
  static final UUID RUN_WITH_WORKLOAD_WITH_DOC_STORE_WORKSPACE_ID = UUID.fromString("480e631f-1c88-4c2d-9081-855981018205");
  static final UUID RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID = UUID.fromString("1bdcfb61-219b-4290-be4f-12f9ac5461be");

  @BeforeAll
  static void init() throws URISyntaxException, IOException, InterruptedException, ApiException {
    testResources.init();
  }

  @BeforeEach
  void setup() throws SQLException, URISyntaxException, IOException {
    testResources.setup();
  }

  @AfterEach
  void tearDown() {
    testResources.tearDown();
  }

  @AfterAll
  static void end() {
    testResources.end();
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testIncrementalSyncWithWorkloadWithoutOutputDocStore() throws Exception {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.getApiClient().getWorkspaceApi()
        .createWorkspaceIfNotExist(new WorkspaceCreateWithId()
            .id(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID)
            .email("acceptance-tests@airbyte.io")
            .name("Airbyte Acceptance Tests" + UUID.randomUUID()));

    testResources.runSmallSyncForAWorkspaceId(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID);
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testIncrementalSyncWithWorkloadWithOutputDocStore() throws Exception {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.getApiClient().getWorkspaceApi()
        .createWorkspaceIfNotExist(new WorkspaceCreateWithId()
            .id(RUN_WITH_WORKLOAD_WITH_DOC_STORE_WORKSPACE_ID)
            .email("acceptance-tests@airbyte.io")
            .name("Airbyte Acceptance Tests" + UUID.randomUUID()));

    testResources.runSmallSyncForAWorkspaceId(RUN_WITH_WORKLOAD_WITH_DOC_STORE_WORKSPACE_ID);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = KUBE,
                                matches = TRUE)
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DISABLE_TEMPORAL_TESTS_IN_GKE)
  void testDestinationCheckConnectionWithWorkload() throws ApiException, URISyntaxException, IOException, SQLException {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.getApiClient().getWorkspaceApi()
        .createWorkspaceIfNotExist(new WorkspaceCreateWithId()
            .id(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID)
            .email("acceptance-tests@airbyte.io")
            .name("Airbyte Acceptance Tests" + UUID.randomUUID()));

    final UUID destinationId = testResources.getTestHarness().createPostgresDestination(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID).getDestinationId();

    final CheckConnectionRead.StatusEnum checkOperationStatus = testResources.getTestHarness().checkDestination(destinationId);

    Assertions.assertNotNull(checkOperationStatus);
    Assertions.assertEquals(StatusEnum.SUCCEEDED, checkOperationStatus);
  }

}
