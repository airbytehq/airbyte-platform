/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

/**
 * Feature-Flag definitions are defined in this file.
 */

/**
 * If enabled, all messages from the source to the destination will be logged in 1 second intervals.
 *
 * This is a permanent flag and would implement the [Permanent] type once converted from an environment-variable.
 */
object LogConnectorMessages : EnvVar(envVar = "LOG_CONNECTOR_MESSAGES")

object StreamCapableState : EnvVar(envVar = "USE_STREAM_CAPABLE_STATE")
object AutoDetectSchema : EnvVar(envVar = "AUTO_DETECT_SCHEMA")
object NeedStateValidation : EnvVar(envVar = "NEED_STATE_VALIDATION")

object RemoveValidationLimit : Temporary<Boolean>(key = "validation.removeValidationLimit", default = false)

object NormalizationInDestination : Temporary<String>(key = "connectors.normalizationInDestination", default = "")

object FieldSelectionEnabled : Temporary<Boolean>(key = "connection.columnSelection", default = false)

object CheckWithCatalog : Temporary<Boolean>(key = "check-with-catalog", default = false)

object MinimumCreditQuantity : Temporary<Int>(key = "minimum-credit-quantity", default = 100)

object ContainerOrchestratorDevImage : Temporary<String>(key = "container-orchestrator-dev-image", default = "")

object ContainerOrchestratorJavaOpts : Temporary<String>(key = "container-orchestrator-java-opts", default = "")

object NewTrialPolicyEnabled : Temporary<Boolean>(key = "billing.newTrialPolicy", default = false)

object AutoPropagateSchema : Temporary<Boolean>(key = "autopropagation.enabled", default = false)

object CheckConnectionUseApiEnabled : Temporary<Boolean>(key = "check-connection-use-api", default = false)

object CheckConnectionUseChildWorkflowEnabled : Temporary<Boolean>(key = "check-connection-use-child-workflow", default = false)

object ShouldRunOnGkeDataplane : Temporary<Boolean>(key="should-run-on-gke-dataplane", default = false)

object ShouldRunOnExpandedGkeDataplane : Temporary<Boolean>(key="should-run-on-expanded-gke-dataplane", default = false)

object ShouldRunRefreshSchema : Temporary<Boolean>(key="should-run-refresh-schema", default = true)
/**
 * The default value is 3 hours, it is larger than what is configured by default in the airbyte self owned instance.
 * The goal is to allow more room for OSS deployment that airbyte can not monitor.
 */
object HeartbeatMaxSecondsBetweenMessages : Permanent<String>(key = "heartbeat-max-seconds-between-messages", default = "10800")

object ShouldFailSyncIfHeartbeatFailure : Permanent<Boolean>(key = "heartbeat.failSync", default = true)

object ConnectorVersionOverride : Permanent<String>(key = "connectors.versionOverrides", default = "")

object RefreshSchemaPeriod : Temporary<Int>(key= "refreshSchema.period.hours", default = 24)

object ConcurrentSourceStreamRead : Temporary<Boolean>(key = "concurrent.source.stream.read", default = false)

object ConcurrentSocatResources : Temporary<String>(key = "concurrent.socat.resources", default = "")

object ReplicationWorkerImpl : Permanent<String>(key = "platform.replication-worker-impl", default = "buffered")

object CheckReplicationProgress : Temporary<Boolean>(key="check-replication-progress", default = false)

// NOTE: this is deprecated in favor of FieldSelectionEnabled and will be removed once that flag is fully deployed.
object FieldSelectionWorkspaces : EnvVar(envVar = "FIELD_SELECTION_WORKSPACES") {
  override fun enabled(ctx: Context): Boolean {
    val enabledWorkspaceIds: List<String> = fetcher(key)
      ?.takeIf { it.isNotEmpty() }
      ?.split(",")
      ?: listOf()

    val contextWorkspaceIds: List<String> = when (ctx) {
      is Multi -> ctx.fetchContexts<Workspace>().map { it.key }
      is Workspace -> listOf(ctx.key)
      else -> listOf()
    }

    return when (contextWorkspaceIds.any { it in enabledWorkspaceIds }) {
      true -> true
      else -> default
    }
  }

  object UnlimitedCredits : Temporary<String>(key = "unlimited-credits", default = "")

  object ConnectorOAuthConsentDisabled : Permanent<Boolean>(key = "connectors.oauth.disableOAuthConsent", default = false)

  object AddSchedulingJitter : Temporary<Boolean>(key = "platform.add-scheduling-jitter", default = false)
}
