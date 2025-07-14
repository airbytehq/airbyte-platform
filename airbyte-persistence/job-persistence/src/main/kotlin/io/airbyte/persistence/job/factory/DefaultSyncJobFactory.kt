/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import com.google.common.collect.Lists
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.DefaultJobCreator
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.helper.model.JobCreatorInput
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.UUID

/**
 * Creates a sync job record in the db.
 */
class DefaultSyncJobFactory(
  private val connectorSpecificResourceDefaultsEnabled: Boolean,
  private val jobCreator: DefaultJobCreator,
  private val oAuthConfigSupplier: OAuthConfigSupplier,
  private val configInjector: ConfigInjector,
  private val workspaceHelper: WorkspaceHelper,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val connectionService: ConnectionService,
  private val operationService: OperationService,
  private val workspaceService: WorkspaceService,
) : SyncJobFactory {
  override fun createSync(
    connectionId: UUID,
    isScheduled: Boolean,
  ): Long {
    try {
      val jobCreatorInput = getJobCreatorInput(connectionId)

      return jobCreator
        .createSyncJob(
          jobCreatorInput.source,
          jobCreatorInput.destination,
          jobCreatorInput.standardSync,
          jobCreatorInput.sourceDockerImageName,
          jobCreatorInput.sourceDockerImageIsDefault,
          jobCreatorInput.sourceProtocolVersion,
          jobCreatorInput.destinationDockerImageName,
          jobCreatorInput.destinationDockerImageIsDefault,
          jobCreatorInput.destinationProtocolVersion,
          jobCreatorInput.standardSyncOperations,
          jobCreatorInput.webhookOperationConfigs,
          jobCreatorInput.sourceDefinition,
          jobCreatorInput.destinationDefinition,
          jobCreatorInput.sourceDefinitionVersion,
          jobCreatorInput.destinationDefinitionVersion,
          jobCreatorInput.workspaceId,
          isScheduled,
        ).orElseThrow { IllegalStateException("We shouldn't be trying to create a new sync job if there is one running already.") }
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: JsonValidationException) {
      throw RuntimeException(e)
    } catch (e: ConfigNotFoundException) {
      throw RuntimeException(e)
    } catch (e: io.airbyte.data.ConfigNotFoundException) {
      throw RuntimeException(e)
    }
  }

  override fun createRefresh(
    connectionId: UUID,
    streamsToRefresh: List<StreamRefresh>,
  ): Long {
    try {
      val jobCreatorInput = getJobCreatorInput(connectionId)

      return jobCreator
        .createRefreshConnection(
          jobCreatorInput.source,
          jobCreatorInput.destination,
          jobCreatorInput.standardSync,
          jobCreatorInput.sourceDockerImageName,
          jobCreatorInput.sourceProtocolVersion,
          jobCreatorInput.destinationDockerImageName,
          jobCreatorInput.destinationProtocolVersion,
          jobCreatorInput.standardSyncOperations,
          jobCreatorInput.webhookOperationConfigs,
          jobCreatorInput.sourceDefinition,
          jobCreatorInput.destinationDefinition,
          jobCreatorInput.sourceDefinitionVersion,
          jobCreatorInput.destinationDefinitionVersion,
          jobCreatorInput.workspaceId,
          streamsToRefresh,
        ).orElseThrow { IllegalStateException("We shouldn't be trying to create a new sync job if there is one running already.") }
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: JsonValidationException) {
      throw RuntimeException(e)
    } catch (e: ConfigNotFoundException) {
      throw RuntimeException(e)
    } catch (e: io.airbyte.data.ConfigNotFoundException) {
      throw RuntimeException(e)
    }
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  private fun getJobCreatorInput(connectionId: UUID): JobCreatorInput {
    val standardSync = connectionService.getStandardSync(connectionId)
    val workspaceId = workspaceHelper.getWorkspaceForSourceId(standardSync.sourceId)
    val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
    val sourceConnection = sourceService.getSourceConnection(standardSync.sourceId)
    val destinationConnection = destinationService.getDestinationConnection(standardSync.destinationId)
    val sourceConfiguration =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceConnection.sourceDefinitionId,
        sourceConnection.sourceId,
        sourceConnection.workspaceId,
        sourceConnection.configuration,
      )
    sourceConnection.withConfiguration(configInjector.injectConfig(sourceConfiguration, sourceConnection.sourceDefinitionId))
    val destinationConfiguration =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destinationConnection.destinationDefinitionId,
        destinationConnection.destinationId,
        destinationConnection.workspaceId,
        destinationConnection.configuration,
      )
    destinationConnection
      .withConfiguration(configInjector.injectConfig(destinationConfiguration, destinationConnection.destinationDefinitionId))
    val sourceDefinition =
      sourceService
        .getStandardSourceDefinition(sourceConnection.sourceDefinitionId)
    val destinationDefinition =
      destinationService
        .getStandardDestinationDefinition(destinationConnection.destinationDefinitionId)

    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, standardSync.sourceId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, standardSync.destinationId)

    val sourceImageName = sourceVersion.dockerRepository + ":" + sourceVersion.dockerImageTag
    val destinationImageName = destinationVersion.dockerRepository + ":" + destinationVersion.dockerImageTag

    val sourceImageVersionDefault =
      actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId)
    val destinationImageVersionDefault =
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId)

    val standardSyncOperations: MutableList<StandardSyncOperation> = Lists.newArrayList()
    for (operationId in standardSync.operationIds) {
      val standardSyncOperation = operationService.getStandardSyncOperation(operationId)
      standardSyncOperations.add(standardSyncOperation)
    }

    // for OSS users, make it possible to ignore default actor-level resource requirements
    if (!connectorSpecificResourceDefaultsEnabled) {
      sourceDefinition.resourceRequirements = null
      destinationDefinition.resourceRequirements = null
    }

    return JobCreatorInput(
      sourceConnection,
      destinationConnection,
      standardSync,
      sourceImageName,
      imageIsDefault(sourceImageName, sourceImageVersionDefault),
      Version(sourceVersion.protocolVersion),
      destinationImageName,
      imageIsDefault(destinationImageName, destinationImageVersionDefault),
      Version(destinationVersion.protocolVersion),
      standardSyncOperations,
      workspace.webhookOperationConfigs,
      sourceDefinition,
      destinationDefinition,
      sourceVersion,
      destinationVersion,
      workspaceId,
    )
  }

  fun imageIsDefault(
    imageName: String?,
    imageVersionDefault: ActorDefinitionVersion?,
  ): Boolean {
    if (imageName == null || imageVersionDefault == null) {
      // We assume that if these values are not set there is no override and therefore the version is
      // default
      return true
    }
    val dockerRepository = imageVersionDefault.dockerRepository
    val dockerImageTag = imageVersionDefault.dockerImageTag
    if (dockerRepository == null || dockerImageTag == null) {
      return true
    }
    return imageName == "$dockerRepository:$dockerImageTag"
  }
}
