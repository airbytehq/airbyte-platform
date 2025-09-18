/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.util.UUID

/**
 * Tests for operations utilizing the workload api / launcher. As development continues on these
 * components, and we migrate more operations to them, we will run these tests in CI to catch
 * regressions.
 */
@Tags(Tag("sync"))
internal class WorkloadBasicAcceptanceTests {
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

  // todo (cgardens) -- don't know why there are 2 @aftereach methods in this class.
  @AfterEach
  fun end() {
    testResources.end()
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
    testResources.testHarness.createWorkspaceWithId(REPLICATION_WORKSPACE_ID)

    testResources.runSmallSyncForAWorkspaceId(REPLICATION_WORKSPACE_ID)
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
    testResources.testHarness!!.createWorkspaceWithId(CHECK_WORKSPACE_ID)

    val destinationId = testResources.testHarness!!.createPostgresDestination(CHECK_WORKSPACE_ID).destinationId

    val checkOperationStatus = testResources.testHarness!!.checkDestination(destinationId)

    Assertions.assertNotNull(checkOperationStatus)
    Assertions.assertEquals(CheckConnectionRead.Status.SUCCEEDED, checkOperationStatus)
  }

  @EnabledIfEnvironmentVariable(named = AcceptanceTestsResources.KUBE, matches = AcceptanceTestsResources.TRUE)
  @DisabledIfEnvironmentVariable(
    named = IS_GKE,
    matches = AcceptanceTestsResources.TRUE,
    disabledReason = AcceptanceTestsResources.DISABLE_TEMPORAL_TESTS_IN_GKE,
  )
  @Test
  @Throws(
    Exception::class,
  )
  fun testDiscover() {
    // Create workspace with static ID for test which is used in the flags.yaml to perform an override
    // in order to exercise the workload path.

    val workspaceId = UUID.randomUUID()
    testResources.testHarness!!.createWorkspaceWithId(workspaceId)

    val actual: AirbyteCatalog?
    val sourceId = testResources.testHarness!!.createPostgresSource(workspaceId).sourceId

    actual = testResources.testHarness!!.discoverSourceSchema(sourceId)

    testResources.testHarness!!.compareCatalog(actual)
  }

  companion object {
    val REPLICATION_WORKSPACE_ID: UUID = UUID.fromString("3d2985a0-a412-45f4-9124-e15800b739be")
    val CHECK_WORKSPACE_ID: UUID = UUID.fromString("1bdcfb61-219b-4290-be4f-12f9ac5461be")
    val DISCOVER_WORKSPACE_ID: UUID = UUID.fromString("3851861d-ac0b-440c-bd60-408cf9e7fc0e")
  }
}
