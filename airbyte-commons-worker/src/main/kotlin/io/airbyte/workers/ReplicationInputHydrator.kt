/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.ConnectionAndJobIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.client.model.generated.ConnectionStateType
import io.airbyte.api.client.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.api.client.model.generated.StreamAttemptMetadata
import io.airbyte.api.client.model.generated.SyncInput
import io.airbyte.commons.converters.ApiClientConverters.Companion.toInternal
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.converters.StateConverter.fromClientToApi
import io.airbyte.commons.converters.StateConverter.toClient
import io.airbyte.commons.converters.StateConverter.toInternal
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.State
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.helpers.CatalogTransforms.updateCatalogForReset
import io.airbyte.config.helpers.StateMessageHelper.getState
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import io.airbyte.config.secrets.buildConfigWithSecretRefsJava
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags.CONNECTION_ID
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_IMAGE
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_TYPE
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.helper.BackfillHelper
import io.airbyte.workers.helper.MapperSecretHydrationHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.github.oshai.kotlinlogging.KotlinLogging
import secrets.persistence.SecretCoordinateException
import java.io.IOException
import java.lang.String
import java.time.Duration
import java.util.UUID
import kotlin.Any
import kotlin.Boolean
import kotlin.Exception
import kotlin.Long
import kotlin.Throws
import kotlin.requireNotNull

class ReplicationInputHydrator(
  private val airbyteApiClient: AirbyteApiClient,
  private val resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper,
  private val mapperSecretHydrationHelper: MapperSecretHydrationHelper,
  private val backfillHelper: BackfillHelper,
  private val catalogClientConverters: CatalogClientConverters,
  private val mapper: ReplicationInputMapper,
  private val metricClient: MetricClient,
  private val connectorSecretsHydrator: ConnectorSecretsHydrator,
  private val useRuntimeSecretPersistence: Boolean,
) {
  private fun <T> retry(supplier: CheckedSupplier<T>): T =
    Failsafe
      .with(
        RetryPolicy
          .builder<Any>()
          .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
          .withMaxRetries(5)
          .build(),
      ).get(supplier)

  private fun refreshSecretsReferences(parsed: ReplicationActivityInput) {
    val jobInput =
      retry {
        // TODO is this still relevant? Most input computation should be tied to the commands. We should look into how to delete this endpoint.
        airbyteApiClient.jobsApi.getJobInput(
          SyncInput(
            parsed.jobRunConfig!!.jobId.toLong(),
            parsed.jobRunConfig!!.attemptId.toInt(),
          ),
        )
      }

    if (jobInput != null) {
      val apiResult = Jsons.convertValue(jobInput, JobInput::class.java)
      if (apiResult?.syncInput != null) {
        val syncInput = apiResult.syncInput

        if (syncInput!!.sourceConfiguration != null) {
          parsed.sourceConfiguration = syncInput.sourceConfiguration
        }

        if (syncInput.destinationConfiguration != null) {
          parsed.destinationConfiguration = syncInput.destinationConfiguration
        }
      }
    }
  }

  /**
   * Converts a ReplicationActivityInput -- passed through Temporal to the replication activity -- to
   * a ReplicationInput which will be passed down the stack to the actual
   * source/destination/orchestrator processes.
   *
   * @param replicationActivityInput the input passed from the sync workflow to the replication
   * activity
   * @return the input to be passed down to the source/destination/orchestrator processes
   * @throws Exception from the Airbyte API
   */
  @Throws(Exception::class)
  fun getHydratedReplicationInput(replicationActivityInput: ReplicationActivityInput): ReplicationInput {
    addTagsToTrace(java.util.Map.of("api_base_url", airbyteApiClient.destinationApi.baseUrl))
    refreshSecretsReferences(replicationActivityInput)

    // Retrieve the connection, which we need in a few places.
    val jobId = replicationActivityInput.jobRunConfig!!.jobId.toLong()
    val connectionInfo =
      if (replicationActivityInput.supportsRefreshes) {
        airbyteApiClient.connectionApi
          .getConnectionForJob(ConnectionAndJobIdRequestBody(replicationActivityInput.connectionId!!, jobId))
      } else {
        airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(replicationActivityInput.connectionId!!))
      }

    val catalog = retrieveCatalog(connectionInfo)
    if (replicationActivityInput.isReset!!) {
      // If this is a reset, we need to set the streams being reset to Full Refresh | Overwrite.
      updateCatalogForReset(replicationActivityInput, catalog)
    }
    // Retrieve the state.
    var state = retrieveState(replicationActivityInput)
    var streamsToBackfill: List<StreamDescriptor>? = null
    if (backfillHelper.syncShouldBackfill(replicationActivityInput, connectionInfo)) {
      streamsToBackfill = backfillHelper.getStreamsToBackfill(replicationActivityInput.schemaRefreshOutput!!.appliedDiff, catalog)
      state =
        getUpdatedStateForBackfill(state, replicationActivityInput.schemaRefreshOutput, replicationActivityInput.connectionId!!, catalog)
    }

    try {
      trackBackfillAndResume(
        jobId,
        replicationActivityInput.jobRunConfig!!.attemptId,
        resumableFullRefreshStatsHelper.getStreamsWithStates(state).stream().toList(),
        streamsToBackfill,
      )
    } catch (e: Exception) {
      log.error(
        "Failed to track stream metadata for connectionId:{} attempt:{}",
        replicationActivityInput.connectionId,
        replicationActivityInput.jobRunConfig!!.attemptId,
        e,
      )
    }

    // Hydrate the secrets.
    val organizationId = replicationActivityInput.connectionContext!!.organizationId
    val workspaceId = replicationActivityInput.connectionContext!!.workspaceId
    val hydrationContext = SecretHydrationContext(organizationId, workspaceId)

    val fullDestinationConfig: JsonNode?
    val fullSourceConfig: JsonNode?

    try {
      val destConfig =
        buildConfigWithSecretRefsJava(replicationActivityInput.destinationConfiguration!!)
      fullDestinationConfig = connectorSecretsHydrator.hydrateConfig(destConfig, hydrationContext)
    } catch (e: SecretCoordinateException) {
      metricClient.count(
        OssMetricsRegistry.SECRETS_HYDRATION_FAILURE,
        1L,
        MetricAttribute(CONNECTOR_IMAGE, replicationActivityInput.destinationLauncherConfig!!.dockerImage),
        MetricAttribute(CONNECTOR_TYPE, ActorType.DESTINATION.toString()),
        MetricAttribute(CONNECTION_ID, replicationActivityInput.destinationLauncherConfig!!.connectionId.toString()),
      )
      throw e
    }

    try {
      val sourceConfig =
        buildConfigWithSecretRefsJava(replicationActivityInput.sourceConfiguration!!)
      fullSourceConfig = connectorSecretsHydrator.hydrateConfig(sourceConfig, hydrationContext)
    } catch (e: SecretCoordinateException) {
      metricClient.count(
        OssMetricsRegistry.SECRETS_HYDRATION_FAILURE,
        1L,
        MetricAttribute(CONNECTOR_IMAGE, replicationActivityInput.sourceLauncherConfig!!.dockerImage),
        MetricAttribute(CONNECTOR_TYPE, ActorType.SOURCE.toString()),
        MetricAttribute(CONNECTION_ID, replicationActivityInput.sourceLauncherConfig!!.connectionId.toString()),
      )
      throw e
    }

    // Hydrate mapper secrets
    val hydratedCatalog =
      mapperSecretHydrationHelper.hydrateMapperSecrets(catalog, useRuntimeSecretPersistence, organizationId)

    return mapper
      .toReplicationInput(replicationActivityInput)
      .withSourceConfiguration(fullSourceConfig)
      .withDestinationConfiguration(fullDestinationConfig)
      .withCatalog(hydratedCatalog)
      .withState(state)
      .withDestinationSupportsRefreshes(replicationActivityInput.supportsRefreshes)
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun trackBackfillAndResume(
    jobId: Long,
    attemptNumber: Long,
    streamsWithStates: List<StreamDescriptor>?,
    streamsToBackfill: List<StreamDescriptor>?,
  ) {
    val metadataPerStream: MutableMap<StreamDescriptor, StreamAttemptMetadata> =
      streamsWithStates
        ?.associateWith { stream ->
          StreamAttemptMetadata(
            streamName = stream.name,
            wasBackfilled = false,
            wasResumed = true,
            streamNamespace = stream.namespace,
          )
        }?.toMutableMap() ?: mutableMapOf()

    streamsToBackfill?.forEach { stream ->
      val existing = metadataPerStream[stream]
      metadataPerStream[stream] =
        if (existing == null) {
          StreamAttemptMetadata(streamName = stream.name, wasBackfilled = true, wasResumed = false, streamNamespace = stream.namespace)
        } else {
          StreamAttemptMetadata(streamName = stream.name, wasBackfilled = true, wasResumed = true, streamNamespace = stream.namespace)
        }
    }

    airbyteApiClient.attemptApi.saveStreamMetadata(
      SaveStreamAttemptMetadataRequestBody(
        jobId,
        attemptNumber.toInt(),
        metadataPerStream.values.toList(),
      ),
    )
  }

  @Throws(Exception::class)
  private fun getUpdatedStateForBackfill(
    state: State?,
    schemaRefreshOutput: RefreshSchemaActivityOutput?,
    connectionId: UUID,
    catalog: ConfiguredAirbyteCatalog,
  ): State? {
    if (schemaRefreshOutput?.appliedDiff != null) {
      val streamsToBackfill: List<StreamDescriptor?> = backfillHelper.getStreamsToBackfill(schemaRefreshOutput.appliedDiff, catalog)
      log.debug(
        "Backfilling streams: {}",
        String.join(", ", streamsToBackfill.stream().map { obj: StreamDescriptor? -> obj!!.name }.toList()),
      )
      val resetState = backfillHelper.clearStateForStreamsToBackfill(state, streamsToBackfill)
      if (resetState != null) {
        // We persist the state here in case the attempt fails, the subsequent attempt will continue the
        // backfill process.
        // TODO(mfsiega-airbyte): move all of the state handling into a separate activity.
        log.debug { "Resetting state for connection: $connectionId" }
        persistState(resetState, connectionId)
      }

      return resetState
    }
    return state
  }

  private fun retrieveCatalog(connectionInfo: ConnectionRead): ConfiguredAirbyteCatalog {
    requireNotNull(connectionInfo.syncCatalog) { "Connection is missing catalog, which is required" }
    val catalog =
      catalogClientConverters.toConfiguredAirbyteInternal(connectionInfo.syncCatalog)
    return catalog
  }

  @Throws(IOException::class)
  private fun persistState(
    resetState: State,
    connectionId: UUID,
  ) {
    val stateWrapper = getTypedState(resetState.state).get()
    val connectionState = toClient(connectionId, stateWrapper)

    airbyteApiClient.stateApi.createOrUpdateState(ConnectionStateCreateOrUpdate(connectionId, connectionState))
  }

  @Throws(IOException::class)
  private fun retrieveState(replicationActivityInput: ReplicationActivityInput): State? {
    val connectionState =
      airbyteApiClient.stateApi.getState(ConnectionIdRequestBody(replicationActivityInput.connectionId!!))
    val state =
      if (connectionState != null && ConnectionStateType.NOT_SET != connectionState.stateType) {
        getState(toInternal(fromClientToApi(connectionState)))
      } else {
        null
      }
    return state
  }

  @Throws(IOException::class)
  private fun updateCatalogForReset(
    replicationActivityInput: ReplicationActivityInput,
    catalog: ConfiguredAirbyteCatalog,
  ) {
    val jobInfo =
      airbyteApiClient.jobsApi.getLastReplicationJob(
        ConnectionIdRequestBody(replicationActivityInput.connectionId!!),
      )
    val hasStreamsToReset = jobInfo?.job != null && jobInfo.job!!.resetConfig != null && jobInfo.job!!.resetConfig!!.streamsToReset != null
    if (hasStreamsToReset) {
      val streamsToReset =
        jobInfo.job!!
          .resetConfig!!
          .streamsToReset!!
          .stream()
          .map { obj: io.airbyte.api.client.model.generated.StreamDescriptor -> obj.toInternal() }
          .toList()
      updateCatalogForReset(streamsToReset, catalog)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
