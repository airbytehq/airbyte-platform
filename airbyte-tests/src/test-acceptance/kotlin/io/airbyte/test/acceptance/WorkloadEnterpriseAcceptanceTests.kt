/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody
import io.airbyte.api.client.model.generated.LogEvent
import io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.io.IOException
import java.util.UUID

/**
 * Tests for operations utilizing the workload api / launcher. As development continues on these
 * components, and we migrate more operations to them, we will run these tests in CI to catch
 * regressions.
 */
@Tags(Tag("enterprise"))
internal class WorkloadEnterpriseAcceptanceTests {
  var testResources: AcceptanceTestsResources = AcceptanceTestsResources()

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    testResources.init()
    testResources.setup()
  }

  @AfterEach
  fun tearDown() {
    testResources.tearDown()
  }

  @AfterEach
  fun end() {
    testResources.end()
  }

  @Throws(IOException::class)
  fun ranWithWorkload(
    connectionId: UUID?,
    jobId: Long,
    attemptNumber: Int,
  ): Boolean {
    val attempt =
      testResources.testHarness!!.apiClient.attemptApi.getAttemptForJob(
        GetAttemptStatsRequestBody(jobId, attemptNumber),
      )
    val creatingWorkloadLog = "Starting workload heartbeat"
    return attempt.logs!!
      .events
      .stream()
      .anyMatch { l: LogEvent -> l.message.contains(creatingWorkloadLog) }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = AcceptanceTestsResources.KUBE, matches = AcceptanceTestsResources.TRUE)
  @DisabledIfEnvironmentVariable(
    named = IS_GKE,
    matches = AcceptanceTestsResources.TRUE,
    disabledReason = AcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE,
  )
  @Throws(
    Exception::class,
  )
  fun testSyncWithWorkload() {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an
    // override in order to exercise the workload path.
    testResources.testHarness!!.createWorkspaceWithId(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID)
    val syncIds =
      testResources.runSmallSyncForAWorkspaceId(RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID)
    Assertions.assertTrue(
      ranWithWorkload(syncIds.connectionId, syncIds.jobId, syncIds.attemptNumber),
      (
        "Failed for workspace:" + RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID + " connection:" + syncIds.connectionId + " job:" +
          syncIds.jobId
      ),
    )
  }

  @Test
  @EnabledIfEnvironmentVariable(named = AcceptanceTestsResources.KUBE, matches = AcceptanceTestsResources.TRUE)
  @DisabledIfEnvironmentVariable(
    named = IS_GKE,
    matches = AcceptanceTestsResources.TRUE,
    disabledReason = AcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE,
  )
  @Throws(
    Exception::class,
  )
  fun testDestinationCheckConnectionWithWorkload() {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.testHarness!!.createWorkspaceWithId(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID)

    val destinationId = testResources.testHarness!!.createPostgresDestination(RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID).destinationId

    val checkOperationStatus = testResources.testHarness!!.checkDestination(destinationId)

    Assertions.assertNotNull(checkOperationStatus)
    Assertions.assertEquals(CheckConnectionRead.Status.SUCCEEDED, checkOperationStatus)
  }

  @Test
  @EnabledIfEnvironmentVariable(named = AcceptanceTestsResources.KUBE, matches = AcceptanceTestsResources.TRUE)
  @DisabledIfEnvironmentVariable(
    named = IS_GKE,
    matches = AcceptanceTestsResources.TRUE,
    disabledReason = AcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE,
  )
  @Throws(
    Exception::class,
  )
  fun testDiscoverSourceSchema() {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.
    testResources.testHarness!!.createWorkspaceWithId(RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID)

    val sourceId = testResources.testHarness!!.createPostgresSource(RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID).sourceId

    val actual = testResources.testHarness!!.discoverSourceSchema(sourceId)

    testResources.testHarness!!.compareCatalog(actual)
  }

  companion object {
    val RUN_WITH_WORKLOAD_WITHOUT_DOC_STORE_WORKSPACE_ID: UUID = UUID.fromString("3d2985a0-a412-45f4-9124-e15800b739be")
    val RUN_CHECK_WITH_WORKLOAD_WORKSPACE_ID: UUID = UUID.fromString("1bdcfb61-219b-4290-be4f-12f9ac5461be")
    val RUN_DISCOVER_WITH_WORKLOAD_WORKSPACE_ID: UUID = UUID.fromString("3851861d-ac0b-440c-bd60-408cf9e7fc0e")
  }
}
