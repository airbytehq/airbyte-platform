/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.dispatch

import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.InjectAwsSecretsToConnectorPods
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseDirectCheck
import io.airbyte.featureflag.UseManifestServerCheck
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.featureflag.isListedAirbyteConnector
import io.airbyte.featureflag.parseDirectCheckConnectors
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

  // The name predates the flag listing connectors instead of images; it is kept because it is a metric tag value.
  IMAGE_NOT_LISTED(
    "its connector isn't in platform.direct-check-images for this data plane group, and platform.use-manifest-server-check is off",
  ),
  NETWORK_SECURITY_TOKENS("it has network security tokens, which only its pod's network policy labels allow"),
  WORKSPACE_FLAG_OFF("platform.use-direct-check is off for its workspace"),
  RUNTIME_SECRET_PERSISTENCE("its organization uses runtime secret persistence"),
  CUSTOM_CONNECTOR("it's a customer's own connector; only Airbyte's connectors in the connector registry are sent to the manifest server"),
  AWS_ROLE_INJECTION("platform.inject-aws-secrets-to-connector-pods is on for its workspace, and only its pod gets those credentials"),
}

/**
 * Decides whether a check runs on the direct job runner or in a pod ([eligibility]), and hands eligible checks to the
 * runner ([dispatch]).
 *
 * A check is eligible when the runner can run it and its workspace has opted in:
 * - Airbyte's image of a connector in [DirectCheckImages] for this data plane group ([isListedAirbyteConnector]),
 *   with [UseDirectCheck] on; or
 * - another of Airbyte's own connectors, with [UseManifestServerCheck] on. The runner looks the connector version up in
 *   the connector registry and runs it on the data plane's manifest server if it is manifest-only and the server
 *   supports its CDK version; it rejects the rest, which then run in a pod. The launcher can't tell from the image
 *   alone, so the runner decides.
 *
 * Either way, the runner must also run it the way a pod would: checks with network security tokens (the pod's
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
    // The data plane identity the launcher registered with at startup; the connector list is set per data plane group,
    // so a check can't be routed before the launcher knows its group.
    val dataplaneGroupId =
      identityService.authNDrivenDataplaneConfig?.dataplaneGroupId ?: return DirectCheckEligibility.NO_DATAPLANE_IDENTITY

    // The same workspace the check pod's env vars are evaluated with (RuntimeEnvVarFactory.checkConnectorEnvVars).
    // IntegrationLauncherConfig is a generated Java model, so the id can be null.
    val workspaceId: UUID? = input.launcherConfig.workspaceId
    val image = input.launcherConfig.dockerImage
    val routeEligibility =
      when {
        isListedAirbyteConnector(image, directCheckConnectors(dataplaneGroupId)) -> directCheckEligibility(workspaceId)
        // Any other connector reaches the runner only through the manifest server, so with that flag off it isn't listed
        // anywhere the runner takes checks from.
        workspaceId == null || !featureFlagClient.boolVariation(UseManifestServerCheck, Workspace(workspaceId)) ->
          DirectCheckEligibility.IMAGE_NOT_LISTED
        else -> manifestServerEligibility(input, workspaceId)
      }
    if (routeEligibility != DirectCheckEligibility.ELIGIBLE) {
      return routeEligibility
    }

    // The pod gets network policy labels for these tokens; the shared runner pod can't, so it would reach the wrong
    // set of networks. StandardCheckConnectionInput is a generated Java model, so the list can be null.
    val networkSecurityTokens: List<String>? = input.checkConnectionInput.networkSecurityTokens
    if (!networkSecurityTokens.isNullOrEmpty()) {
      return DirectCheckEligibility.NETWORK_SECURITY_TOKENS
    }

    // A check pod picks its secret store per organization (RuntimeEnvVarFactory.getSecretPersistenceEnvVars); the
    // runner's hydrator uses one setting for every check.
    if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(input.getOrganizationId()))) {
      return DirectCheckEligibility.RUNTIME_SECRET_PERSISTENCE
    }
    return DirectCheckEligibility.ELIGIBLE
  }

  // A connector baked into the runner's image, which it runs as a local process.
  private fun directCheckEligibility(workspaceId: UUID?): DirectCheckEligibility {
    if (workspaceId == null || !featureFlagClient.boolVariation(UseDirectCheck, Workspace(workspaceId))) {
      return DirectCheckEligibility.WORKSPACE_FLAG_OFF
    }
    return DirectCheckEligibility.ELIGIBLE
  }

  // One of Airbyte's own connectors, which the runner may run on the data plane's manifest server. A customer's
  // connector isn't in the connector registry, so there is no published manifest to run.
  private fun manifestServerEligibility(
    input: CheckConnectionInput,
    workspaceId: UUID,
  ): DirectCheckEligibility {
    // IntegrationLauncherConfig is a generated Java model, so the flag can be null; only a known Airbyte connector is
    // offered to the runner.
    val isCustomConnector: Boolean? = input.launcherConfig.isCustomConnector
    if (isCustomConnector != false) {
      return DirectCheckEligibility.CUSTOM_CONNECTOR
    }
    // The check pod gets AWS credentials and the workspace's external ID for assuming a customer's role
    // (RuntimeEnvVarFactory.resolveAwsAssumedRoleEnvVars); the manifest server only gets the connector config.
    if (featureFlagClient.boolVariation(InjectAwsSecretsToConnectorPods, Workspace(workspaceId))) {
      return DirectCheckEligibility.AWS_ROLE_INJECTION
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

  private fun directCheckConnectors(dataplaneGroupId: UUID): Set<String> =
    parseDirectCheckConnectors(featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)))

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
