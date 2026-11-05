/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.dispatch

import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseDirectCheck
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.input.getOrganizationId
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

/** Whether a check can run on the direct job runner, and if not, why; [reason] is written to the launcher's log. */
enum class DirectCheckEligibility(
  val reason: String,
) {
  ELIGIBLE("eligible for the direct job runner"),
  NO_DATAPLANE_IDENTITY("the launcher doesn't know its data plane group yet"),
  IMAGE_NOT_LISTED("its image isn't in platform.direct-check-images for this data plane group"),
  NETWORK_SECURITY_TOKENS("it has network security tokens, which only its pod's network policy labels allow"),
  WORKSPACE_FLAG_OFF("platform.use-direct-check is off for its workspace"),
  RUNTIME_SECRET_PERSISTENCE("its organization uses runtime secret persistence"),
}

/**
 * Decides whether a check runs on the direct job runner or in a pod ([eligibility]), and hands eligible checks to the
 * runner ([dispatch]).
 *
 * A check is eligible when its image is in [DirectCheckImages] for this data plane group, [UseDirectCheck] is on for
 * its workspace, and the runner can run it the way a pod would: checks with network security tokens (the pod's
 * network policy labels) or from an organization on runtime secret persistence stay on pods. When the runner does
 * not take an eligible check, it falls back to a pod, so the runner being down never fails a check.
 */
@Singleton
class DirectCheckRouter(
  private val directJobRunnerClient: DirectJobRunnerClient,
  private val workloadApiClient: WorkloadApiClient,
  private val identityService: DataplaneIdentityService,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
) {
  fun eligibility(input: CheckConnectionInput): DirectCheckEligibility {
    // The data plane identity the launcher registered with at startup; the image list is set per data plane group,
    // so a check can't be routed before the launcher knows its group.
    val dataplaneGroupId =
      identityService.authNDrivenDataplaneConfig?.dataplaneGroupId ?: return DirectCheckEligibility.NO_DATAPLANE_IDENTITY
    if (input.launcherConfig.dockerImage !in directCheckImages(dataplaneGroupId)) {
      return DirectCheckEligibility.IMAGE_NOT_LISTED
    }

    // The pod gets network policy labels for these tokens; the shared runner pod can't, so it would reach the wrong
    // set of networks. StandardCheckConnectionInput is a generated Java model, so the list can be null.
    val networkSecurityTokens: List<String>? = input.checkConnectionInput.networkSecurityTokens
    if (!networkSecurityTokens.isNullOrEmpty()) {
      return DirectCheckEligibility.NETWORK_SECURITY_TOKENS
    }

    // The same workspace the check pod's env vars are evaluated with (RuntimeEnvVarFactory.checkConnectorEnvVars).
    // IntegrationLauncherConfig is a generated Java model, so the id can be null.
    val workspaceId: UUID? = input.launcherConfig.workspaceId
    if (workspaceId == null || !featureFlagClient.boolVariation(UseDirectCheck, Workspace(workspaceId))) {
      return DirectCheckEligibility.WORKSPACE_FLAG_OFF
    }

    // A check pod picks its secret store per organization (RuntimeEnvVarFactory.getSecretPersistenceEnvVars); the
    // runner's hydrator uses one setting for every check.
    if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(input.getOrganizationId()))) {
      return DirectCheckEligibility.RUNTIME_SECRET_PERSISTENCE
    }
    return DirectCheckEligibility.ELIGIBLE
  }

  /** Hands an eligible check to the runner. Returns false when the runner did not take it and it needs a pod. */
  fun dispatch(msg: LauncherInput): Boolean {
    val dispatchStatus = directJobRunnerClient.dispatch(msg)
    if (dispatchStatus !is DirectJobDispatch.Rejected) {
      return true
    }

    if (hasRunnerStartedJob(msg, dispatchStatus)) {
      logger.info { "The direct job runner is running check ${msg.workloadId} despite the failed handoff" }
      return true
    }

    logger.info { "Running check ${msg.workloadId} in a pod: the direct job runner did not take it (${dispatchStatus.reason})" }
    metricClient.count(
      OssMetricsRegistry.DIRECT_JOB_DISPATCH,
      1,
      MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, msg.workloadType.toString()),
      MetricAttribute(MetricTags.RESULT_TAG, "fallback"),
      MetricAttribute(MetricTags.FALLBACK_REASON_TAG, dispatchStatus.reason),
    )
    return false
  }

  private fun directCheckImages(dataplaneGroupId: UUID): List<String> =
    featureFlagClient
      .stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId))
      .split(",")
      .map { it.trim() }
      .filter { it.isNotEmpty() }

  // A lost response can hide a runner that already marked the workload RUNNING; a pod then would run it twice. If the
  // status can't be read, the pod runs it: a check run twice is better than a check that never runs.
  private fun hasRunnerStartedJob(
    msg: LauncherInput,
    dispatchStatus: DirectJobDispatch.Rejected,
  ): Boolean {
    if (dispatchStatus.reason == "not_configured") {
      return false
    }
    return try {
      workloadApiClient.workloadGet(msg.workloadId).status == WorkloadStatus.RUNNING
    } catch (e: Exception) {
      logger.warn(e) { "Could not read the status of check ${msg.workloadId} after a failed handoff" }
      false
    }
  }
}
