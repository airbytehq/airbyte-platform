/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
object AutoDetectSchema : EnvVar(envVar = "AUTO_DETECT_SCHEMA")
object NeedStateValidation : EnvVar(envVar = "NEED_STATE_VALIDATION")

object RemoveValidationLimit : Temporary<Boolean>(key = "validation.removeValidationLimit", default = false)

object NormalizationInDestination : Temporary<String>(key = "connectors.normalizationInDestination", default = "")

object FieldSelectionEnabled : Temporary<Boolean>(key = "connection.columnSelection", default = false)

object CheckWithCatalog : Temporary<Boolean>(key = "check-with-catalog", default = false)

object ContainerOrchestratorDevImage : Temporary<String>(key = "container-orchestrator-dev-image", default = "")

object ContainerOrchestratorJavaOpts : Temporary<String>(key = "container-orchestrator-java-opts", default = "")

object NewTrialPolicyEnabled : Temporary<Boolean>(key = "billing.newTrialPolicy", default = false)

object CanonicalCatalogSchema : Temporary<Boolean>(key = "canonical-catalog-schema", default = false)

object CatalogCanonicalJson : Temporary<Boolean>(key = "catalog-canonical-json", default = false)

object EarlySyncEnabled : Temporary<Boolean>(key = "billing.early-sync-enabled", default = false)

object FetchEarlySyncJobs : Temporary<Boolean>(key = "billing.fetch-early-sync-jobs", default = false)

object ShouldRunOnGkeDataplane : Temporary<Boolean>(key = "should-run-on-gke-dataplane", default = false)

object ShouldRunOnExpandedGkeDataplane : Temporary<Boolean>(key = "should-run-on-expanded-gke-dataplane", default = false)

object ShouldRunRefreshSchema : Temporary<Boolean>(key = "should-run-refresh-schema", default = true)

object AutoBackfillOnNewColumns : Temporary<Boolean>(key = "platform.auto-backfill-on-new-columns", default = false)

object ResetBackfillState : Temporary<Boolean>(key = "platform.reset-backfill-state", default = false)

/**
 * The default value is 3 hours, it is larger than what is configured by default in the airbyte self owned instance.
 * The goal is to allow more room for OSS deployment that airbyte can not monitor.
 */
object HeartbeatMaxSecondsBetweenMessages : Permanent<String>(key = "heartbeat-max-seconds-between-messages", default = "10800")

object ShouldFailSyncIfHeartbeatFailure : Permanent<Boolean>(key = "heartbeat.failSync", default = true)

object ConnectorVersionOverride : Permanent<String>(key = "connectors.versionOverrides", default = "")

object DestinationTimeoutEnabled : Permanent<Boolean>(key = "destination-timeout-enabled", default = true)

object ShouldFailSyncOnDestinationTimeout : Permanent<Boolean>(key = "destination-timeout.failSync", default = true)

object DestinationTimeoutSeconds : Permanent<Int>(key = "destination-timeout.seconds", default = 7200)

object UseActorScopedDefaultVersions : Temporary<Boolean>(key = "connectors.useActorScopedDefaultVersions", default = true)

object RunSupportStateUpdater : Temporary<Boolean>(key = "connectors.runSupportStateUpdater", default = true)

object NotifyOnConnectorBreakingChanges : Temporary<Boolean>(key = "connectors.notifyOnConnectorBreakingChanges", default = true)

object NotifyBreakingChangesOnSupportStateUpdate : Temporary<Boolean>(key = "connectors.notifyBreakingChangesOnSupportStateUpdate", default = true)

object RefreshSchemaPeriod : Temporary<Int>(key = "refreshSchema.period.hours", default = 24)

object ConcurrentSourceStreamRead : Temporary<Boolean>(key = "concurrent.source.stream.read", default = false)

object ReplicationWorkerImpl : Permanent<String>(key = "platform.replication-worker-impl", default = "buffered")

object UseResourceRequirementsVariant : Permanent<String>(key = "platform.resource-requirements-variant", default = "default")

object UseParallelStreamStatsTracker : Temporary<Boolean>(key = "platform.use-parallel-stream-stats-tracker", default = false)

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

object SunsetFCP : Temporary<Boolean>(key = "platform.sunset-fcp", default = false)

object DestResourceOverrides : Temporary<String>(key = "dest-resource-overrides", default = "")

object OrchestratorResourceOverrides : Temporary<String>(key = "orchestrator-resource-overrides", default = "")

object SourceResourceOverrides : Temporary<String>(key = "source-resource-overrides", default = "")

object ConnectorApmEnabled : Permanent<Boolean>(key = "connectors.apm-enabled", default = false)

object AutoRechargeEnabled : Permanent<Boolean>(key = "billing.autoRecharge", default = false)

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

  object UnlimitedCredits : Temporary<String>(key = "unlimited-credits", default = "")

  object ConnectorOAuthConsentDisabled : Permanent<Boolean>(key = "connectors.oauth.disableOAuthConsent", default = false)

  object AddSchedulingJitter : Temporary<Boolean>(key = "platform.add-scheduling-jitter", default = false)

  object UseNewSchemaUpdateNotification : Temporary<Boolean>(key = "platform.use-new-schema-update-notification", default = false)
}

object RunSocatInConnectorContainer : Temporary<Boolean>(key = "platform.run-socat-in-connector-container", default = false)

object FailSyncIfTooBig : Temporary<Boolean>(key = "platform.fail-sync-if-too-big", default = false)

object DefaultOrgForNewWorkspace : Temporary<Boolean>(key = "platform.set-default-org-for-new-workspace", default = false)

object WorkloadHeartbeatRate : Permanent<Int>(key = "workload.heartbeat.rate", default = 5)

object WorkloadPollingInterval : Permanent<Int>(key = "workload.polling.interval", default = 30)

/**
 * Duration in minutes. This should always be less than the value for [io.airbyte.cron.jobs.WorkloadMonitor.heartbeatTimeout]
 */
object WorkloadHeartbeatTimeout : Permanent<Int>(key = "workload.heartbeat.timeout", default = 4)

object UseNewCronScheduleCalculation : Temporary<Boolean>(key = "platform.use-new-cron-schedule-calculation", default = false)

object UseRuntimeSecretPersistence : Temporary<Boolean>(key = "platform.use-runtime-secret-persistence", default = false)

object UseWorkloadApi : Temporary<Boolean>(key = "platform.use-workload-api", default = false)

object AddInitialCreditsForWorkspace : Temporary<Int>(key = "add-credits-at-workspace-creation-for-org", default = 0)

object WorkloadApiRouting : Permanent<String>(key = "workload-api-routing", default = "workload_default")

object FailMissingPks : Temporary<Boolean>(key = "platform.fail-missing-pks", default = false)

object PrintLongRecordPks : Temporary<Boolean>(key = "platform.print-long-record-pks", default = false)

object TrackCommittedStatsWhenUsingGlobalState : Temporary<Boolean>(key = "global-state-committed-stats-tracking-enabled", default = false)

object UseWorkloadOutputDocStore : Temporary<Boolean>(key = "platform.use-workload-output-doc-store", default = false)

object UseWorkloadApiForCheck : Temporary<Boolean>(key = "platform.use-workload-api-for-check", default = false)

object WorkloadCheckFrequencyInSeconds : Permanent<Int>(key = "platform.workload-check-frequency-in-seconds", default = 1)
