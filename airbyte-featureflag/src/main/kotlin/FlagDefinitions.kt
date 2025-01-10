/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

/**
 * Feature-Flag definitions are defined in this file.
 */

package io.airbyte.featureflag

/**
 * If enabled, all messages from the source to the destination will be logged in 1 second intervals.
 *
 * This is a permanent flag and would implement the [Permanent] type once converted from an environment-variable.
 */
object LogConnectorMessages : EnvVar(envVar = "LOG_CONNECTOR_MESSAGES")

object RemoveValidationLimit : Temporary<Boolean>(key = "validation.removeValidationLimit", default = false)

object FieldSelectionEnabled : Temporary<Boolean>(key = "connection.columnSelection", default = false)

object CheckWithCatalog : Temporary<Boolean>(key = "check-with-catalog", default = false)

object ContainerOrchestratorDevImage : Temporary<String>(key = "container-orchestrator-dev-image", default = "")

object ContainerOrchestratorJavaOpts : Temporary<String>(key = "container-orchestrator-java-opts", default = "")

object EarlySyncEnabled : Temporary<Boolean>(key = "billing.early-sync-enabled", default = false)

object ShouldRunRefreshSchema : Temporary<Boolean>(key = "should-run-refresh-schema", default = true)

/**
 * The default value is 3 hours, it is larger than what is configured by default in the airbyte self owned instance.
 * The goal is to allow more room for OSS deployment that airbyte can not monitor.
 */
object HeartbeatMaxSecondsBetweenMessages : Permanent<String>(key = "heartbeat-max-seconds-between-messages", default = "10800")

object ShouldFailSyncIfHeartbeatFailure : Permanent<Boolean>(key = "heartbeat.failSync", default = false)

object DestinationTimeoutEnabled : Permanent<Boolean>(key = "destination-timeout-enabled", default = true)

object ShouldFailSyncOnDestinationTimeout : Permanent<Boolean>(key = "destination-timeout.failSync", default = true)

object DestinationTimeoutSeconds : Permanent<Int>(key = "destination-timeout.seconds", default = 7200)

object NotifyOnConnectorBreakingChanges : Temporary<Boolean>(key = "connectors.notifyOnConnectorBreakingChanges", default = true)

object NotifyBreakingChangesOnSupportStateUpdate : Temporary<Boolean>(key = "connectors.notifyBreakingChangesOnSupportStateUpdate", default = true)

object UseBreakingChangeScopes : Temporary<Boolean>(key = "connectors.useBreakingChangeScopes", default = true)

object RefreshSchemaPeriod : Temporary<Int>(key = "refreshSchema.period.hours", default = 24)

object ConcurrentSourceStreamRead : Temporary<Boolean>(key = "concurrent.source.stream.read", default = false)

object UseResourceRequirementsVariant : Permanent<String>(key = "platform.resource-requirements-variant", default = "default")

object SuccessiveCompleteFailureLimit : Temporary<Int>(key = "complete-failures.max-successive", default = -1)

object TotalCompleteFailureLimit : Temporary<Int>(key = "complete-failures.max-total", default = -1)

object SuccessivePartialFailureLimit : Temporary<Int>(key = "partial-failures.max-successive", default = -1)

object TotalPartialFailureLimit : Temporary<Int>(key = "partial-failures.max-total", default = -1)

object CompleteFailureBackoffMinInterval : Temporary<Int>(key = "complete-failures.backoff.min-interval-s", default = -1)

object CompleteFailureBackoffMaxInterval : Temporary<Int>(key = "complete-failures.backoff.max-interval-s", default = -1)

object CompleteFailureBackoffBase : Temporary<Int>(key = "complete-failures.backoff.base", default = -1)

object UseCustomK8sScheduler : Temporary<String>(key = "platform.use-custom-k8s-scheduler", default = "")

object HideActorDefinitionFromList : Permanent<Boolean>(key = "connectors.hideActorDefinitionFromList", default = false)

object PauseSyncsWithUnsupportedActors : Temporary<Boolean>(key = "connectors.pauseSyncsWithUnsupportedActors", default = true)

object DestResourceOverrides : Temporary<String>(key = "dest-resource-overrides", default = "")

object OrchestratorResourceOverrides : Temporary<String>(key = "orchestrator-resource-overrides", default = "")

object SourceResourceOverrides : Temporary<String>(key = "source-resource-overrides", default = "")

object ConnectorApmEnabled : Permanent<Boolean>(key = "connectors.apm-enabled", default = false)

object BillingMigrationMaintenance : Temporary<Boolean>(key = "billing.migrationMaintenance", default = false)

// NOTE: this is deprecated in favor of FieldSelectionEnabled and will be removed once that flag is fully deployed.
object FieldSelectionWorkspaces : EnvVar(envVar = "FIELD_SELECTION_WORKSPACES") {
  override fun enabled(ctx: Context): Boolean {
    val enabledWorkspaceIds: List<String> =
      fetcher(key)
        ?.takeIf { it.isNotEmpty() }
        ?.split(",")
        ?: listOf()

    val contextWorkspaceIds: List<String> =
      when (ctx) {
        is Multi -> ctx.fetchContexts<Workspace>().map { it.key }
        is Workspace -> listOf(ctx.key)
        else -> listOf()
      }

    return when (contextWorkspaceIds.any { it in enabledWorkspaceIds }) {
      true -> true
      else -> default
    }
  }

  object ConnectorOAuthConsentDisabled : Permanent<Boolean>(key = "connectors.oauth.disableOAuthConsent", default = false)

  object AddSchedulingJitter : Temporary<Boolean>(key = "platform.add-scheduling-jitter", default = false)
}

object DefaultOrgForNewWorkspace : Temporary<Boolean>(key = "platform.set-default-org-for-new-workspace", default = false)

object WorkloadHeartbeatRate : Permanent<Int>(key = "workload.heartbeat.rate", default = 5)

/**
 * Defines whether a workload launcher should be consuming tasks.
 */
object WorkloadLauncherConsumerEnabled : Permanent<Boolean>(key = "workload-launcher-consumer-enabled", default = true)

object WorkloadPollingInterval : Permanent<Int>(key = "workload.polling.interval", default = 30)

/**
 * Duration in minutes. This should always be less than the value for [io.airbyte.cron.jobs.WorkloadMonitor.heartbeatTimeout]
 */
object WorkloadHeartbeatTimeout : Permanent<Int>(key = "workload.heartbeat.timeout", default = 4)

object UseNewCronScheduleCalculation : Temporary<Boolean>(key = "platform.use-new-cron-schedule-calculation", default = false)

object UseRuntimeSecretPersistence : Temporary<Boolean>(key = "platform.use-runtime-secret-persistence", default = false)

object EmitStateStatsToSegment : Temporary<Boolean>(key = "platform.emit-state-stats-segment", default = false)

object LogStreamNamesInSateMessage : Temporary<Boolean>(key = "platform.logs-stream-names-state", default = false)

object WorkloadApiRouting : Permanent<String>(key = "workload-api-routing", default = "workload_default")

object PrintLongRecordPks : Temporary<Boolean>(key = "platform.print-long-record-pks", default = false)

object InjectAwsSecretsToConnectorPods : Temporary<Boolean>(key = "platform.inject-aws-secrets-to-connector-pods", default = false)

object WorkloadCheckFrequencyInSeconds : Permanent<Int>(key = "platform.workload-check-frequency-in-seconds", default = 1)

object FailSyncOnInvalidChecksum : Temporary<Boolean>(key = "platform.fail-sync-on-invalid-checksum", default = false)

object HydrateAggregatedStats : Temporary<Boolean>(key = "platform.hydrate-aggregated-stats", default = true)

object WriteOutputCatalogToObjectStorage : Temporary<Boolean>(key = "platform.write-output-catalog-to-object-storage", default = false)

object UseCustomK8sInitCheck : Temporary<Boolean>(key = "platform.use-custom-k8s-init-check", default = true)

object ConnectionFieldLimitOverride : Permanent<Int>(key = "connection-field-limit-override", default = -1)

object EnableResumableFullRefresh : Temporary<Boolean>(key = "platform.enable-resumable-full-refresh", default = false)

object AlwaysRunCheckBeforeSync : Permanent<Boolean>(key = "platform.always-run-check-before-sync", default = false)

object RestrictLoginsForSSODomains : Temporary<Boolean>(key = "platform.restrict-logins-for-sso-domains", default = false)

object ResetStreamsStateWhenDisabled : Temporary<Boolean>(key = "reset-stream-state-on-disable", default = false)

object LogStateMsgs : Temporary<Boolean>(key = "platform.log-state-msgs", default = false)

object ReplicationBufferOverride : Temporary<Int>(key = "platform.replication-buffer-override", default = 0)

object NodeSelectorOverride : Temporary<String>(key = "platform.node-selector-override", default = "")

object ReportConnectorDiskUsage : Temporary<Boolean>(key = "platform.report-connector-disk-usage", default = false)

object PlatformInitContainerImage : Temporary<String>(key = "platform.init-container-image", default = "")

object SubOneHourSyncSchedules : Permanent<Boolean>(key = "platform.allow-sub-one-hour-sync-frequency", default = true)

object AllowMappersDefaultSecretPersistence : Permanent<Boolean>(key = "platform.allow-mappers-default-secret-persistence", default = false)

object RunDeclarativeSourcesUpdater : Permanent<Boolean>(key = "platform.run-declarative-sources-updater", default = true)

object AllowSpotInstances : Temporary<Boolean>(key = "platform.allow-spot-instances", default = false)

object HydrateLimits : Temporary<Boolean>(key = "platform.hydrate.limits", default = false)
