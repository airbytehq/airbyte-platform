/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import io.airbyte.commons.converters.ConfigReplacer
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.helpers.ResourceRequirementsUtils
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.data.repositories.ActorDefinitionRepository
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.CheckConnectionInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.server.exceptions.NotFoundException
import jakarta.inject.Singleton
import java.util.UUID

val log = KotlinLogging.logger { }

interface JobInputService {
  fun getCheckInput(
    actorId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput

  fun getCheckInput(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput

  fun getDiscoveryInput(
    actorId: UUID,
    workspaceId: UUID,
  ): DiscoverCommandInput.DiscoverCatalogInput

  fun getDiscoveryInputWithJobId(
    actorId: UUID,
    workspaceId: UUID,
    jobId: String,
    attemptId: Long,
  ): DiscoverCommandInput.DiscoverCatalogInput
}

@Singleton
class JobInputServiceImpl(
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val actorRepository: ActorRepository,
  private val actorDefinitionRepository: ActorDefinitionRepository,
  private val oAuthConfigSupplier: OAuthConfigSupplier,
  private val configInjector: ConfigInjector,
  private val secretReferenceService: SecretReferenceService,
  private val contextBuilder: ContextBuilder,
  private val scopedConfigurationService: ScopedConfigurationService,
) : JobInputService {
  companion object {
    private val HASH_FUNCTION = Hashing.md5()
  }

  override fun getCheckInput(
    actorId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val actor =
      actorRepository.findByActorId(actorId)
        ?: throw NotFoundException() // Better exception?
    return when (actor.actorType) {
      ActorType.source -> getCheckInputBySourceId(actorId, jobId, attemptId)
      ActorType.destination -> getCheckInputByDestinationId(actorId, jobId, attemptId)
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  override fun getCheckInput(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val actorDefinition =
      actorDefinitionRepository.findByActorDefinitionId(actorDefinitionId)
        ?: throw NotFoundException() // Better exception?

    return when (actorDefinition.actorType) {
      ActorType.source -> getCheckInputBySourceDefinitionId(actorDefinitionId, workspaceId, configuration)
      ActorType.destination -> getCheckInputByDestinationDefinitionId(actorDefinitionId, workspaceId, configuration)
      else -> throw IllegalStateException("Actor type ${actorDefinition.actorType} not supported")
    }
  }

  override fun getDiscoveryInput(
    actorId: UUID,
    workspaceId: UUID,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val actor =
      actorRepository.findByActorId(actorId)
        ?: throw NotFoundException() // Better exception?
    return when (actor.actorType) {
      ActorType.source -> getDiscoverInputBySourceId(sourceId = actorId, jobId = UUID.randomUUID().toString(), attemptId = 0L)
      ActorType.destination -> throw IllegalArgumentException("Discovery is not supported for destination, actorId: $actorId")
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  override fun getDiscoveryInputWithJobId(
    actorId: UUID,
    workspaceId: UUID,
    jobId: String,
    attemptId: Long,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val actor =
      actorRepository.findByActorId(actorId)
        ?: throw NotFoundException() // Better exception?
    return when (actor.actorType) {
      ActorType.source -> getDiscoverInputBySourceId(actorId, jobId, attemptId)
      ActorType.destination -> throw IllegalArgumentException("Discovery is not supported for destination, actorId: $actorId")
      else -> throw IllegalStateException("Actor type ${actor.actorType} not supported")
    }
  }

  private fun getCheckInputBySourceId(
    sourceId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val (source, sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformation(sourceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        source!!.sourceId,
        source!!.workspaceId,
        source!!.configuration,
      )

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.SOURCE,
      definitionId = source.sourceDefinitionId,
      actorId = source.sourceId,
      workspaceId = source.workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromSource(source),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputBySourceDefinitionId(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val (_, sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformationByDefinitionId(sourceDefinitionId, workspaceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.maskSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        configuration,
        sourceDefinitionVersion.spec,
      )

    val jobId = UUID.randomUUID().toString()
    val attemptId = 0L

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.SOURCE,
      definitionId = sourceDefinition.sourceDefinitionId,
      actorId = null,
      workspaceId = workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = null,
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputByDestinationDefinitionId(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
  ): CheckConnectionInput {
    val destinationInformation = getDestinationInformationByDefinitionId(destinationDefinitionId, workspaceId)
    val destinationDefinition = destinationInformation.destinationDefinition

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationInformation.destinationDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.maskDestinationOAuthParameters(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        configuration,
        destinationInformation.destinationDefinitionVersion.spec,
      )

    val jobId = UUID.randomUUID().toString()
    val attemptId = 0L

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.DESTINATION,
      definitionId = destinationDefinition.destinationDefinitionId,
      actorId = null,
      workspaceId = workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(destinationInformation.destinationDefinitionVersion.protocolVersion),
      isCustomConnector = destinationInformation.destinationDefinition.custom,
      resourceRequirements = destinationInformation.resourceRequirements,
      allowedHosts = destinationInformation.destinationDefinitionVersion.allowedHosts,
      actorContext = null,
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun getCheckInputByDestinationId(
    destinationId: UUID,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val destinationInformation = getDestinationInformation(destinationId)
    val destination = destinationInformation.destination
    val destinationDefinition = destinationInformation.destinationDefinition

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationInformation.destinationDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectDestinationOAuthParameters(
        destinationDefinition.destinationDefinitionId,
        destination!!.destinationId,
        destination!!.workspaceId,
        destination!!.configuration,
      )

    return buildJobCheckConnectionConfig(
      actorType = io.airbyte.config.ActorType.DESTINATION,
      definitionId = destination.destinationDefinitionId,
      actorId = destination.destinationId,
      workspaceId = destination.workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      protocolVersion = Version(destinationInformation.destinationDefinitionVersion.protocolVersion),
      isCustomConnector = destinationInformation.destinationDefinition.custom,
      resourceRequirements = destinationInformation.resourceRequirements,
      allowedHosts = destinationInformation.destinationDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromDestination(destination),
      jobId = jobId,
      attemptId = attemptId,
    )
  }

  private fun buildJobCheckConnectionConfig(
    actorType: io.airbyte.config.ActorType,
    definitionId: UUID,
    actorId: UUID?,
    workspaceId: UUID,
    configuration: JsonNode,
    dockerImage: String,
    protocolVersion: Version,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
    allowedHosts: AllowedHosts?,
    actorContext: ActorContext?,
    jobId: String?,
    attemptId: Long?,
  ): CheckConnectionInput {
    val injectedConfig: JsonNode = configInjector.injectConfig(configuration, definitionId)

    val inlinedConfigWithSecretRefs = InlinedConfigWithSecretRefs(injectedConfig)

    val configWithSecrets: ConfigWithSecretReferences =
      if (actorId == null) {
        inlinedConfigWithSecretRefs.toConfigWithRefs()
      } else {
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(actorId),
          injectedConfig,
          WorkspaceId(workspaceId),
        )
      }

    val jobId = jobId ?: UUID.randomUUID().toString()
    val attemptId = attemptId ?: 0L

    val configReplacer = ConfigReplacer()

    return CheckConnectionInput(
      jobRunConfig =
        JobRunConfig()
          .withJobId(jobId)
          .withAttemptId(attemptId),
      launcherConfig =
        IntegrationLauncherConfig()
          .withJobId(jobId)
          .withWorkspaceId(workspaceId)
          .withDockerImage(dockerImage)
          .withProtocolVersion(protocolVersion)
          .withIsCustomConnector(isCustomConnector)
          .withAttemptId(attemptId)
          .withAllowedHosts(configReplacer.getAllowedHosts(allowedHosts, configuration)),
      checkConnectionInput =
        StandardCheckConnectionInput()
          .withActorType(actorType)
          .withActorId(actorId)
          .withConnectionConfiguration(configWithSecrets.config)
          .withResourceRequirements(resourceRequirements)
          .withActorContext(actorContext)
          .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId)),
    )
  }

  private fun buildJobDiscoverConfig(
    actorType: io.airbyte.config.ActorType,
    definitionId: UUID,
    actorId: UUID?,
    workspaceId: UUID,
    configuration: JsonNode,
    dockerImage: String,
    dockerTag: String,
    protocolVersion: Version,
    isCustomConnector: Boolean,
    resourceRequirements: ResourceRequirements?,
    allowedHosts: AllowedHosts,
    actorContext: ActorContext?,
    jobId: String,
    attemptId: Long,
    isManual: Boolean,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val injectedConfig: JsonNode = configInjector.injectConfig(configuration, definitionId)

    val inlinedConfigWithSecretRefs = InlinedConfigWithSecretRefs(injectedConfig)

    val configWithSecrets: ConfigWithSecretReferences =
      if (actorId == null) {
        inlinedConfigWithSecretRefs.toConfigWithRefs()
      } else {
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(actorId),
          injectedConfig,
          WorkspaceId(workspaceId),
        )
      }

    val configReplacer = ConfigReplacer()

    return DiscoverCommandInput.DiscoverCatalogInput(
      jobRunConfig =
        JobRunConfig()
          .withJobId(jobId)
          .withAttemptId(attemptId),
      integrationLauncherConfig =
        IntegrationLauncherConfig()
          .withJobId(jobId)
          .withWorkspaceId(workspaceId)
          .withDockerImage(dockerImage)
          .withProtocolVersion(protocolVersion)
          .withIsCustomConnector(isCustomConnector)
          .withAttemptId(attemptId)
          .withAllowedHosts(configReplacer.getAllowedHosts(allowedHosts, configuration)),
      discoverCatalogInput =
        StandardDiscoverCatalogInput()
          .withSourceId(actorId.toString())
          .withConnectorVersion(dockerTag)
          .withConnectionConfiguration(configWithSecrets.config)
          .withConfigHash(
            HASH_FUNCTION
              .hashBytes(
                Jsons.serialize(configWithSecrets.config).toByteArray(
                  Charsets.UTF_8,
                ),
              ).toString(),
          ).withManual(isManual)
          .withResourceRequirements(resourceRequirements)
          .withActorContext(actorContext)
          .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId)),
    )
  }

  private fun getDiscoverInputBySourceId(
    sourceId: UUID,
    jobId: String,
    attemptId: Long,
  ): DiscoverCommandInput.DiscoverCatalogInput {
    val (source, sourceDefinition, sourceDefinitionVersion, resourceRequirements) = getSourceInformation(sourceId)

    val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceDefinitionVersion)
    val configWithOauthParams: JsonNode =
      oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceDefinition.sourceDefinitionId,
        source!!.sourceId,
        source!!.workspaceId,
        source!!.configuration,
      )

    return buildJobDiscoverConfig(
      actorType = io.airbyte.config.ActorType.SOURCE,
      definitionId = source.sourceDefinitionId,
      actorId = source.sourceId,
      workspaceId = source.workspaceId,
      configuration = configWithOauthParams,
      dockerImage = dockerImage,
      dockerTag = sourceDefinitionVersion.dockerImageTag,
      protocolVersion = Version(sourceDefinitionVersion.protocolVersion),
      isCustomConnector = sourceDefinition.custom,
      resourceRequirements = resourceRequirements,
      allowedHosts = sourceDefinitionVersion.allowedHosts,
      actorContext = contextBuilder.fromSource(source),
      jobId = jobId,
      attemptId = attemptId,
      isManual = true,
    )
  }

  private fun getNetworkSecurityTokens(workspaceId: UUID): List<String> =
    try {
      scopedConfigurationService
        .getScopedConfigurations(NetworkSecurityTokenKey, mapOf(ConfigScopeType.WORKSPACE to workspaceId))
        .map { it.value }
        .toList()
    } catch (e: IllegalArgumentException) {
      log.error { e.message }
      emptyList()
    }

  data class SourceInformation(
    val source: SourceConnection?,
    val sourceDefinition: StandardSourceDefinition,
    val sourceDefinitionVersion: ActorDefinitionVersion,
    val resourceRequirements: ResourceRequirements?,
  )

  private fun getSourceInformation(sourceId: UUID): SourceInformation {
    val source = sourceService.getSourceConnection(sourceId)
    val sourceDefinition = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
    val sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, sourceId)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        sourceDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return SourceInformation(
      source,
      sourceDefinition,
      sourceDefinitionVersion,
      resourceRequirements,
    )
  }

  private fun getSourceInformationByDefinitionId(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
  ): SourceInformation {
    val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    val sourceDefinitionVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, null)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        sourceDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return SourceInformation(
      null,
      sourceDefinition,
      sourceDefinitionVersion,
      resourceRequirements,
    )
  }

  data class DestinationInformation(
    val destination: DestinationConnection?,
    val destinationDefinition: StandardDestinationDefinition,
    val destinationDefinitionVersion: ActorDefinitionVersion,
    val resourceRequirements: ResourceRequirements?,
  )

  private fun getDestinationInformation(destinationId: UUID): DestinationInformation {
    val destination = destinationService.getDestinationConnection(destinationId)
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
    val destinationDefinitionVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destinationId,
      )
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        destinationDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return DestinationInformation(
      destination,
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }

  private fun getDestinationInformationByDefinitionId(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
  ): DestinationInformation {
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
    val destinationDefinitionVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, null)
    val resourceRequirements =
      ResourceRequirementsUtils.getResourceRequirementsForJobType(
        destinationDefinition.resourceRequirements,
        JobTypeResourceLimit.JobType.CHECK_CONNECTION,
      )

    return DestinationInformation(
      null,
      destinationDefinition,
      destinationDefinitionVersion,
      resourceRequirements,
    )
  }
}
