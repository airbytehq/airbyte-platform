/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.FusionEnablementResolveRequest
import io.airbyte.api.client.model.generated.FusionEnablementResolveResponse
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FusionLaunchEnabled
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.airbyte.workload.launcher.constants.EnvVarConstants
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.net.URI

private val logger = KotlinLogging.logger { }

/** Resolves persisted actor facts immediately before a new sync pod is created. */
@Singleton
class FusionLaunchResolver(
  private val apiClient: AirbyteApiClient,
  private val identityService: DataplaneIdentityService,
  private val featureFlagClient: FeatureFlagClient,
  private val edition: AirbyteEdition,
) {
  fun resolve(
    payload: SyncPayload,
    workloadId: String,
  ) {
    // Never retain an earlier resolution when a queued workload is retried or resolution is skipped.
    payload.fusionDestinationEnvironment = emptyMap()
    if (edition != AirbyteEdition.CLOUD) return
    val dataplane = identityService.authNDrivenDataplaneConfig ?: return
    if (!featureFlagClient.boolVariation(FusionLaunchEnabled, DataplaneGroup(dataplane.dataplaneGroupId))) return
    require(dataplane.dataplaneEnabled) { "Dataplane is disabled" }
    val input = payload.input
    val organizationId = input.connectionContext?.organizationId
    if (organizationId == null) {
      logger.warn { "Skipping Fusion resolution for workload $workloadId because its connection context has no organization id" }
      return
    }
    val result =
      apiClient.fusionEnablementResolverApi.resolveFusionEnablementForSync(
        FusionEnablementResolveRequest(
          organizationId = organizationId,
          workspaceId = requireNotNull(input.workspaceId),
          connectionId = requireNotNull(input.connectionId),
          sourceId = requireNotNull(input.sourceId),
          destinationId = requireNotNull(input.destinationId),
          dataplaneGroupId = dataplane.dataplaneGroupId,
          workloadId = workloadId,
        ),
      )
    val environment = result.destinationEnvironment
    if (result.reason != FusionEnablementResolveResponse.Reason.ENABLED) {
      require(!result.injectAwsBootstrapCredentials && environment.isEmpty()) { "Invalid disabled Fusion resolution" }
      return
    }
    require(
      result.injectAwsBootstrapCredentials &&
        environment.keys.containsAll(EnvVarConstants.FUSION_COPY_NAMES) &&
        environment.keys.all { it in EnvVarConstants.FUSION_COPY_NAMES || it == EnvVarConstants.FUSION_COPY_ENDPOINT } &&
        environment.values.all { it.isNotBlank() },
    ) {
      "Incomplete Fusion launch resolution"
    }
    environment[EnvVarConstants.FUSION_COPY_ENDPOINT]?.let { endpoint ->
      val uri = runCatching { URI(endpoint) }.getOrNull()
      require(uri != null && uri.scheme in setOf("http", "https") && !uri.host.isNullOrBlank()) {
        "Invalid Fusion launch copy endpoint"
      }
    }
    val expectedRoleArn = Regex("^arn:aws:iam::[0-9]{12}:role/airbyte-fusion-writer-${Regex.escape(organizationId.toString())}$")
    require(
      environment["AIRBYTE_FUSION_ENABLED"] == "true" &&
        expectedRoleArn.matches(environment["AIRBYTE_FUSION_S3_ROLE_ARN"].orEmpty()),
    ) {
      "Invalid Fusion launch destination identity"
    }
    payload.fusionDestinationEnvironment = environment.toMap()
  }
}
