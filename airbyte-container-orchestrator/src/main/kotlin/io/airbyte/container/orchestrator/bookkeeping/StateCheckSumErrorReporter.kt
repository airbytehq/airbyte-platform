/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.amazonaws.internal.ExceptionUtils
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.ReleaseStage
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.UUID

private val logger = KotlinLogging.logger { }

@Singleton
class StateCheckSumErrorReporter(
  @Named("stateCheckSumErrorReportingClient") private val jobErrorReportingClient: Optional<JobErrorReportingClient>,
  @Value("\${airbyte.version}") private val airbyteVersion: String,
  private val airbyteEdition: Configs.AirbyteEdition,
  private val airbyteApiClient: AirbyteApiClient,
  private val webUrlHelper: WebUrlHelper,
) {
  @Volatile
  private var errorReported = false

  fun reportError(
    workspaceId: UUID,
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    origin: FailureReason.FailureOrigin,
    internalMessage: String,
    externalMessage: String,
    exception: Throwable,
    stateMessage: AirbyteStateMessage,
  ) {
    if (errorReported) {
      return
    }
    try {
      jobErrorReportingClient.ifPresent { client ->
        val standardWorkspace = StandardWorkspace().withWorkspaceId(workspaceId)
        val commonMetadata: Map<String?, String?> =
          mapOf(JobErrorReporter.JOB_ID_KEY to jobId.toString()) +
            getConnectionMetadata(workspaceId, connectionId) +
            getWorkspaceMetadata(workspaceId) +
            airbyteMetadata()

        val metadata: Map<String, String>
        val dockerImageName: String

        when (origin) {
          FailureReason.FailureOrigin.SOURCE -> {
            val sourceId = retry { airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(connectionId)).sourceId }
            val source = retry { airbyteApiClient.sourceApi.getSource(SourceIdRequestBody(sourceId)) }
            val sourceVersion =
              retry { airbyteApiClient.actorDefinitionVersionApi.getActorDefinitionVersionForSourceId(SourceIdRequestBody(sourceId)) }
            val sourceDefinition =
              retry { airbyteApiClient.sourceDefinitionApi.getSourceDefinition(SourceDefinitionIdRequestBody(source.sourceDefinitionId)) }
            dockerImageName = getDockerImageName(sourceVersion.dockerRepository, sourceVersion.dockerImageTag)
            metadata =
              getDefinitionMetadata(sourceDefinition.sourceDefinitionId, sourceDefinition.name, dockerImageName, sourceDefinition.releaseStage)
          }
          FailureReason.FailureOrigin.DESTINATION -> {
            val destinationId = retry { airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(connectionId)).destinationId }
            val destination = retry { airbyteApiClient.destinationApi.getDestination(DestinationIdRequestBody(destinationId)) }
            val destinationVersion =
              retry { airbyteApiClient.actorDefinitionVersionApi.getActorDefinitionVersionForDestinationId(DestinationIdRequestBody(destinationId)) }
            val destinationDefinition =
              retry {
                airbyteApiClient.destinationDefinitionApi.getDestinationDefinition(
                  DestinationDefinitionIdRequestBody(destination.destinationDefinitionId),
                )
              }
            dockerImageName = getDockerImageName(destinationVersion.dockerRepository, destinationVersion.dockerImageTag)
            metadata =
              getDefinitionMetadata(
                destinationDefinition.destinationDefinitionId,
                destinationDefinition.name,
                dockerImageName,
                destinationDefinition.releaseStage,
              )
          }
          else -> {
            logger.info { "Can't use state checksum error reporter for $origin error reporting" }
            return@ifPresent
          }
        }

        val failureReason = createFailureReason(origin, internalMessage, externalMessage, exception, jobId, attemptNumber)
        val allMetadata: Map<String?, String?> = getFailureReasonMetadata(failureReason) + metadata + commonMetadata
        client.reportJobFailureReason(
          standardWorkspace,
          failureReason,
          dockerImageName,
          allMetadata,
          AttemptConfigReportingContext(null, null, State().withState(Jsons.jsonNode(stateMessage))),
        )
      }
    } catch (e: Exception) {
      logger.error(e) { "Error while trying to report state checksum error" }
    }

    errorReported = true
  }

  fun createFailureReason(
    origin: FailureReason.FailureOrigin,
    internalMessage: String,
    externalMessage: String,
    exception: Throwable,
    jobId: Long,
    attemptNumber: Int,
  ): FailureReason =
    FailureReason()
      .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
      .withFailureOrigin(origin)
      .withInternalMessage(internalMessage)
      .withExternalMessage(externalMessage)
      .withTimestamp(Instant.now().toEpochMilli())
      .withStacktrace(ExceptionUtils.exceptionStackTrace(exception))
      .withMetadata(
        Metadata()
          .withAdditionalProperty("jobId", jobId)
          .withAdditionalProperty("attemptNumber", attemptNumber),
      )

  fun getDockerImageName(
    dockerRepository: String,
    dockerImageTag: String,
  ): String = "$dockerRepository:$dockerImageTag"

  fun airbyteMetadata(): Map<String, String> =
    mapOf(
      JobErrorReporter.AIRBYTE_VERSION_META_KEY to airbyteVersion,
      JobErrorReporter.AIRBYTE_EDITION_META_KEY to airbyteEdition.name,
    )

  fun getFailureReasonMetadata(failureReason: FailureReason): Map<String, String> =
    mutableMapOf<String, String>().apply {
      failureReason.failureOrigin?.let { put(JobErrorReporter.FAILURE_ORIGIN_META_KEY, it.value()) }
      failureReason.failureType?.let { put(JobErrorReporter.FAILURE_TYPE_META_KEY, it.value()) }
    }

  fun getDefinitionMetadata(
    definitionId: UUID,
    name: String,
    dockerImage: String,
    releaseStage: ReleaseStage?,
  ): Map<String, String> {
    val connectorRepository = dockerImage.split(":")[0]
    return mutableMapOf(
      JobErrorReporter.CONNECTOR_DEFINITION_ID_META_KEY to definitionId.toString(),
      JobErrorReporter.CONNECTOR_NAME_META_KEY to name,
      JobErrorReporter.CONNECTOR_REPOSITORY_META_KEY to connectorRepository,
    ).apply {
      releaseStage?.let { put(JobErrorReporter.CONNECTOR_RELEASE_STAGE_META_KEY, it.value) }
    }
  }

  fun getConnectionMetadata(
    workspaceId: UUID,
    connectionId: UUID,
  ): Map<String, String> {
    val connectionUrl = webUrlHelper.getConnectionUrl(workspaceId, connectionId)
    return mapOf(
      JobErrorReporter.CONNECTION_ID_META_KEY to connectionId.toString(),
      JobErrorReporter.CONNECTION_URL_META_KEY to connectionUrl,
    )
  }

  fun getWorkspaceMetadata(workspaceId: UUID): Map<String, String> {
    val workspaceUrl = webUrlHelper.getWorkspaceUrl(workspaceId)
    return mapOf(
      JobErrorReporter.WORKSPACE_ID_META_KEY to workspaceId.toString(),
      JobErrorReporter.WORKSPACE_URL_META_KEY to workspaceUrl,
    )
  }

  companion object {
    private fun <T> retry(supplier: CheckedSupplier<T>): T =
      Failsafe
        .with(
          RetryPolicy
            .builder<T>()
            .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
            .withMaxRetries(5)
            .build(),
        ).get(supplier)
  }
}
