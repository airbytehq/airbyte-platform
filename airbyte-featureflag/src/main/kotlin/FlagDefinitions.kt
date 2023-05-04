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

// NOTE: this is deprecated in favor of FieldSelectionEnabled and will be removed once that flag is fully deployed.
object ApplyFieldSelection : EnvVar(envVar = "APPLY_FIELD_SELECTION")

object PerfBackgroundJsonValidation : Temporary<Boolean>(key = "performance.backgroundJsonSchemaValidation", default = false)

object RemoveValidationLimit : Temporary<Boolean>(key = "validation.removeValidationLimit", default = false)

object CommitStatsAsap : Temporary<Boolean>(key = "platform.commitStatsAsap", default = true)

object NormalizationInDestinationBiqQuery : Temporary<Boolean>(key = "connectors.normalizationInDestination.bigquery", default = false)

object FieldSelectionEnabled : Temporary<Boolean>(key = "connection.columnSelection", default = false)

object CheckWithCatalog : Temporary<Boolean>(key = "check-with-catalog", default = false)

object ConnectorVersionOverridesEnabled : Temporary<Boolean>(key = "connectors.versionOverridesEnabled", default = false)

object UseActorDefinitionVersionTableDefaults : Temporary<Boolean>(key = "connectors.useActorDefinitionVersionTableDefaults", default = false)

object MinimumCreditQuantity : Temporary<Int>(key = "minimum-credit-quantity", default = 100)

object ContainerOrchestratorDevImage : Temporary<String>(key = "container-orchestrator-dev-image", default = "")

object UseNotificationWorkflow : Temporary<Boolean>(key = "use-notification-workflow", default = false)

object ContainerOrchestratorJavaOpts : Temporary<String>(key = "container-orchestrator-java-opts", default = "")

object NewTrialPolicyEnabled : Temporary<Boolean>(key = "billing.newTrialPolicy", default = false)

object AutoPropagateSchema : Temporary<Boolean>(key = "autopropagation.enabled", default = false)

object CheckConnectionUseApiEnabled : Temporary<Boolean>(key = "check-connection-use-api", default = false)

object ShouldRunOnGkeDataplane : Temporary<Boolean>(key="should-run-on-gke-dataplane", default = false)
/**
 * The default value is 3 hours, it is larger than what is configured by default in the airbyte self owned instance.
 * The goal is to allow more room for OSS deployment that airbyte can not monitor.
 */
object HeartbeatMaxSecondsBetweenMessages : Permanent<String>(key = "heartbeat-max-seconds-between-messages", default = "10800")

object ShouldFailSyncIfHeartbeatFailure : Permanent<Boolean>(key = "heartbeat.failSync", default = true)


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
}
