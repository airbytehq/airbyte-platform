package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.SyncInput
import io.airbyte.commons.json.Jsons
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectorSidecarFetchesInputFromInit
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.OrchestratorFetchesInputFromInit
import io.airbyte.featureflag.RefreshConfigBeforeSecretHydration
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pipeline.stages.model.WorkloadPayload
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Deserializes input payloads and performs any necessary hydration from other
 * sources. When complete, a fully formed workload input should be attached to
 * the IO.
 */
@Singleton
@Named("build")
open class BuildInputStage(
  private val checkInputHydrator: CheckConnectionInputHydrator,
  private val discoverConnectionInputHydrator: DiscoverCatalogInputHydrator,
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val airbyteApiClient: AirbyteApiClient,
  metricPublisher: CustomMetricPublisher,
  @Value("\${airbyte.data-plane-id}") dataplaneId: String,
  private val featureFlagClient: FeatureFlagClient,
) : LaunchStage(metricPublisher, dataplaneId) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "BuildInputStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MeterFilterFactory.STAGE_NAME_TAG, value = "build")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val built = buildPayload(input.msg.workloadInput, input.msg.workloadType)

    return input.apply {
      payload = built
    }
  }

  override fun getStageName(): StageName {
    return StageName.BUILD
  }

  private fun buildPayload(
    rawPayload: String,
    type: WorkloadType,
  ): WorkloadPayload {
    return when (type) {
      WorkloadType.CHECK -> {
        val parsed: CheckConnectionInput = deserializer.toCheckConnectionInput(rawPayload)
        if (featureFlagClient.boolVariation(ConnectorSidecarFetchesInputFromInit, Workspace(parsed.launcherConfig.workspaceId))) {
          return CheckPayload(parsed)
        }
        val hydrated = parsed.apply { checkConnectionInput = checkInputHydrator.getHydratedStandardCheckInput(parsed.checkConnectionInput) }
        CheckPayload(hydrated)
      }

      WorkloadType.DISCOVER -> {
        val parsed: DiscoverCatalogInput = deserializer.toDiscoverCatalogInput(rawPayload)
        if (featureFlagClient.boolVariation(ConnectorSidecarFetchesInputFromInit, Workspace(parsed.launcherConfig.workspaceId))) {
          return DiscoverCatalogPayload(parsed)
        }
        val hydrated =
          parsed.apply {
            discoverCatalogInput = discoverConnectionInputHydrator.getHydratedStandardDiscoverInput(parsed.discoverCatalogInput)
          }
        DiscoverCatalogPayload(hydrated)
      }

      WorkloadType.SYNC -> {
        val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)
        if (shouldRefreshSecretsReferences(parsed)) {
          refreshSecretsReferences(parsed)
        }
        val ffContext = Multi(listOf(Connection(parsed.connectionId), Workspace(parsed.workspaceId)))

        val hydrated: ReplicationInput =
          if (featureFlagClient.boolVariation(OrchestratorFetchesInputFromInit, ffContext)) {
            replicationInputHydrator.mapActivityInputToReplInput(parsed)
          } else {
            replicationInputHydrator.getHydratedReplicationInput(parsed)
          }
        SyncPayload(hydrated)
      }

      WorkloadType.SPEC -> {
        val parsed: SpecInput = deserializer.toSpecInput(rawPayload)
        // no need to hydrate as spec doesn't require secrets
        SpecPayload(parsed)
      }

      else -> {
        throw NotImplementedError("Unimplemented workload type: $type")
      }
    }
  }

  private fun shouldRefreshSecretsReferences(input: ReplicationActivityInput): Boolean {
    val context =
      Multi(
        listOf(
          Connection(input.connectionId),
          Workspace(input.workspaceId),
        ),
      )
    return featureFlagClient.boolVariation(RefreshConfigBeforeSecretHydration, context)
  }

  /**
   * Regenerates the input to update the source and destination config.
   * This is to reduce instances where the secrets have been updated by another task which lead
   * us to hydrate using outdated secret references.
   */
  private fun refreshSecretsReferences(parsed: ReplicationActivityInput) {
    retry {
      airbyteApiClient.jobsApi.getJobInput(
        SyncInput(
          parsed.jobRunConfig.jobId.toLong(),
          parsed.jobRunConfig.attemptId.toInt(),
        ),
      )
    }?.let {
      val apiResult = Jsons.convertValue(it, JobInput::class.java)
      apiResult?.syncInput?.let { syncInput ->
        syncInput.sourceConfiguration
          ?.let { updatedSourceConfig -> parsed.sourceConfiguration = updatedSourceConfig }
        syncInput.destinationConfiguration
          ?.let { updatedDestinationConfig -> parsed.destinationConfiguration = updatedDestinationConfig }
      }
    }
  }

  companion object {
    private fun <T> retry(supplier: CheckedSupplier<T>): T {
      return Failsafe.with(
        RetryPolicy.builder<T>()
          .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
          .withMaxRetries(5)
          .build(),
      )
        .get(supplier)
    }
  }
}
