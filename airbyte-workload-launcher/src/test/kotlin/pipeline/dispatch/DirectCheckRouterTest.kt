/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.dispatch

import fixtures.RecordFixtures
import io.airbyte.config.ActorContext
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseDirectCheck
import io.airbyte.featureflag.UseManifestServerCheck
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

private const val IMAGE = "airbyte/source-postgres:3.8.10"
private const val MANIFEST_ONLY_IMAGE = "airbyte/source-bing-ads:3.0.21"

class DirectCheckRouterTest {
  private val dataplaneGroupId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val msg = RecordFixtures.launcherInput("check-1", workloadType = WorkloadType.CHECK)

  private val directJobRunnerClient = mockk<DirectJobRunnerClient>()
  private val workloadApiClient = mockk<WorkloadApiClient>()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val identityService =
    mockk<DataplaneIdentityService> {
      every { authNDrivenDataplaneConfig } returns
        DataplaneConfig(
          dataplaneId = UUID.randomUUID(),
          dataplaneName = "dp",
          dataplaneEnabled = true,
          dataplaneGroupId = dataplaneGroupId,
          dataplaneGroupName = "group",
          organizationId = null,
        )
    }
  private val featureFlagClient =
    mockk<FeatureFlagClient> {
      every { stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns " source-mysql , source-postgres "
      every { boolVariation(UseDirectCheck, Workspace(workspaceId)) } returns true
      every { boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns false
      every { boolVariation(UseRuntimeSecretPersistence, Organization(organizationId)) } returns false
      every { boolVariation(InjectAwsSecretsToConnectorPods, Workspace(workspaceId)) } returns false
    }

  private val router = DirectCheckRouter(directJobRunnerClient, workloadApiClient, identityService, featureFlagClient, metricClient)

  private fun input(
    image: String = IMAGE,
    workspace: UUID? = workspaceId,
    networkSecurityTokens: List<String> = emptyList(),
    isCustomConnector: Boolean? = false,
  ) = CheckConnectionInput(
    jobRunConfig = JobRunConfig(),
    launcherConfig =
      IntegrationLauncherConfig().withDockerImage(image).withWorkspaceId(workspace).withIsCustomConnector(isCustomConnector),
    checkConnectionInput =
      StandardCheckConnectionInput()
        .withActorContext(ActorContext().withWorkspaceId(workspace).withOrganizationId(organizationId))
        .withNetworkSecurityTokens(networkSecurityTokens),
  )

  private fun verifyFallback(reason: String) =
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_JOB_DISPATCH,
        1,
        MetricAttribute("workload_type", "check"),
        MetricAttribute("result", "fallback"),
        MetricAttribute("fallback_reason", reason),
      )
    }

  @Test
  fun `a listed connector with the workspace flag on is eligible`() {
    assertEquals(DirectCheckEligibility.ELIGIBLE, router.eligibility(input()))
  }

  @Test
  fun `a listed connector is eligible at any version`() {
    assertEquals(DirectCheckEligibility.ELIGIBLE, router.eligibility(input(image = "airbyte/source-postgres:3.8.9")))
    assertEquals(DirectCheckEligibility.ELIGIBLE, router.eligibility(input(image = "airbyte/source-postgres:3.9.0-rc1")))
  }

  @Test
  fun `a connector that isn't listed for the data plane group is not eligible`() {
    assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input(image = "airbyte/source-mssql:4.1.0")))
  }

  @Test
  fun `only Airbyte's own image of a listed connector is eligible`() {
    listOf(
      "acme/source-postgres:3.8.10",
      "registry.example.com:5000/airbyte/source-postgres:3.8.10",
      "airbyte/source-postgres:3.8.10@sha256:0123456789abcdef",
      "airbyte/source-postgres",
      "airbyte/source-postgres:",
    ).forEach { image ->
      assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input(image = image)), image)
    }
  }

  @Test
  fun `a flag value still listing images matches nothing, so those checks stay on pods`() {
    every { featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns IMAGE

    assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input()))
  }

  @Test
  fun `an empty image list makes nothing eligible`() {
    every { featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns ""

    assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input()))
  }

  @Test
  fun `the workspace flag off is not eligible`() {
    every { featureFlagClient.boolVariation(UseDirectCheck, Workspace(workspaceId)) } returns false

    assertEquals(DirectCheckEligibility.WORKSPACE_FLAG_OFF, router.eligibility(input()))
  }

  @Test
  fun `a check without a workspace is not eligible`() {
    assertEquals(DirectCheckEligibility.WORKSPACE_FLAG_OFF, router.eligibility(input(workspace = null)))
  }

  @Test
  fun `a launcher without a data plane identity routes nothing`() {
    every { identityService.authNDrivenDataplaneConfig } returns null

    assertEquals(DirectCheckEligibility.NO_DATAPLANE_IDENTITY, router.eligibility(input()))
  }

  @Test
  fun `a check with network security tokens is not eligible, since only its pod gets the network policy labels`() {
    assertEquals(DirectCheckEligibility.NETWORK_SECURITY_TOKENS, router.eligibility(input(networkSecurityTokens = listOf("token-1"))))
  }

  @Test
  fun `an organization on runtime secret persistence is not eligible`() {
    every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId)) } returns true

    assertEquals(DirectCheckEligibility.RUNTIME_SECRET_PERSISTENCE, router.eligibility(input()))
  }

  @Test
  fun `with the manifest server flag on, any of Airbyte's own connectors is offered to the runner`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns true

    assertEquals(DirectCheckEligibility.ELIGIBLE, router.eligibility(input(image = MANIFEST_ONLY_IMAGE)))
  }

  @Test
  fun `with the manifest server flag off, an unlisted image stays on a pod`() {
    assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input(image = MANIFEST_ONLY_IMAGE)))
  }

  @Test
  fun `a check without a workspace is never offered to the manifest server`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, any()) } returns true

    assertEquals(DirectCheckEligibility.IMAGE_NOT_LISTED, router.eligibility(input(image = MANIFEST_ONLY_IMAGE, workspace = null)))
  }

  @Test
  fun `a customer's connector, or one not known to be Airbyte's, stays on a pod`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns true

    assertEquals(DirectCheckEligibility.CUSTOM_CONNECTOR, router.eligibility(input(image = MANIFEST_ONLY_IMAGE, isCustomConnector = true)))
    assertEquals(DirectCheckEligibility.CUSTOM_CONNECTOR, router.eligibility(input(image = MANIFEST_ONLY_IMAGE, isCustomConnector = null)))
  }

  @Test
  fun `a manifest server check with network security tokens stays on a pod`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns true

    assertEquals(
      DirectCheckEligibility.NETWORK_SECURITY_TOKENS,
      router.eligibility(input(image = MANIFEST_ONLY_IMAGE, networkSecurityTokens = listOf("token-1"))),
    )
  }

  @Test
  fun `a manifest server check from an organization on runtime secret persistence stays on a pod`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns true
    every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId)) } returns true

    assertEquals(DirectCheckEligibility.RUNTIME_SECRET_PERSISTENCE, router.eligibility(input(image = MANIFEST_ONLY_IMAGE)))
  }

  @Test
  fun `a manifest server check from a workspace whose pods get injected AWS credentials stays on a pod`() {
    every { featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) } returns true
    every { featureFlagClient.boolVariation(InjectAwsSecretsToConnectorPods, Workspace(workspaceId)) } returns true

    assertEquals(DirectCheckEligibility.AWS_ROLE_INJECTION, router.eligibility(input(image = MANIFEST_ONLY_IMAGE)))
  }

  @Test
  fun `an accepted handoff runs on the runner`() {
    every { directJobRunnerClient.dispatch(msg) } returns DirectJobDispatch.Accepted

    assertTrue(router.dispatch(msg))
  }

  @Test
  fun `a rejected handoff falls back to a pod and is recorded`() {
    for (reason in listOf("unreachable", "rejected", "not_configured")) {
      every { directJobRunnerClient.dispatch(msg) } returns DirectJobDispatch.Rejected(reason)
      every { workloadApiClient.workloadGet(msg.workloadId) } returns Workload(status = WorkloadStatus.CLAIMED)

      assertFalse(router.dispatch(msg), reason)

      verifyFallback(reason)
    }
  }

  @Test
  fun `a failed handoff the runner already started is not run again in a pod`() {
    every { directJobRunnerClient.dispatch(msg) } returns DirectJobDispatch.Rejected("unreachable")
    every { workloadApiClient.workloadGet(msg.workloadId) } returns Workload(status = WorkloadStatus.RUNNING)

    assertTrue(router.dispatch(msg))

    verify(exactly = 0) { metricClient.count(any(), any(), *anyVararg()) }
  }

  @Test
  fun `a status lookup that fails after a rejected handoff still falls back to a pod`() {
    every { directJobRunnerClient.dispatch(msg) } returns DirectJobDispatch.Rejected("unreachable")
    every { workloadApiClient.workloadGet(msg.workloadId) } throws RuntimeException("workload api down")

    assertFalse(router.dispatch(msg))

    verifyFallback("unreachable")
  }

  @Test
  fun `a runner that isn't configured falls back without a status lookup`() {
    every { directJobRunnerClient.dispatch(msg) } returns DirectJobDispatch.Rejected("not_configured")

    assertFalse(router.dispatch(msg))

    verify(exactly = 0) { workloadApiClient.workloadGet(any()) }
    verifyFallback("not_configured")
  }
}
