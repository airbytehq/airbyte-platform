/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.BaseActorDefinitionVersionInfo
import io.airbyte.api.model.generated.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.model.generated.BuilderProjectForDefinitionResponse
import io.airbyte.api.model.generated.BuilderProjectOauthConsentRequest
import io.airbyte.api.model.generated.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.ConnectorBuilderAuxiliaryRequest
import io.airbyte.api.model.generated.ConnectorBuilderHttpRequest
import io.airbyte.api.model.generated.ConnectorBuilderHttpResponse
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetailsRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectFullResolveRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectFullResolveResponse
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadSlicesInnerPagesInner
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.model.generated.ContributionInfo
import io.airbyte.api.model.generated.DeclarativeManifestBaseImageRead
import io.airbyte.api.model.generated.DeclarativeManifestRead
import io.airbyte.api.model.generated.DeclarativeManifestRequestBody
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SourceDefinitionIdBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.model.generated.FailedPreconditionData
import io.airbyte.api.problems.throwable.generated.FailedPreconditionProblem
import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.handlers.helpers.BuilderProjectUpdater
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.mapToCompleteOAuthResponse
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi
import io.airbyte.connectorbuilderserver.api.client.model.generated.AuxiliaryRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse
import io.airbyte.connectorbuilderserver.api.client.model.generated.ResolveManifest
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTOR_BUILDER_PROJECT_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToRootSpan
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.CollectionUtils
import jakarta.annotation.Nullable
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Map
import java.util.Objects
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * ConnectorBuilderProjectsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
open class ConnectorBuilderProjectsHandler
  @Inject
  constructor(
    private val declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
    private val connectorBuilderService: ConnectorBuilderService,
    private val buildProjectUpdater: BuilderProjectUpdater,
    @param:Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
    private val manifestInjector: DeclarativeSourceManifestInjector,
    private val workspaceService: WorkspaceService,
    private val featureFlagClient: FeatureFlagClient,
    private val secretsRepositoryReader: SecretsRepositoryReader,
    private val secretsRepositoryWriter: SecretsRepositoryWriter,
    private val secretPersistenceConfigService: SecretPersistenceConfigService,
    private val sourceService: SourceService,
    @param:Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
    private val connectorBuilderServerApiClient: ConnectorBuilderServerApi,
    private val actorDefinitionService: ActorDefinitionService,
    private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
    @param:Named("oauthImplementationFactory") private val oAuthImplementationFactory: OAuthImplementationFactory,
    private val metricClient: MetricClient,
  ) {
    private fun getProjectDetailsWithoutBaseAdvInfo(project: ConnectorBuilderProject): ConnectorBuilderProjectDetailsRead {
      val detailsRead =
        ConnectorBuilderProjectDetailsRead()
          .name(project.name)
          .updatedAt(project.updatedAt)
          .builderProjectId(project.builderProjectId)
          .sourceDefinitionId(project.actorDefinitionId)
          .activeDeclarativeManifestVersion(
            project.activeDeclarativeManifestVersion,
          ).hasDraft(project.hasDraft)
          .componentsFileContent(project.componentsFileContent)

      if (project.contributionPullRequestUrl != null) {
        detailsRead.contributionInfo =
          ContributionInfo()
            .pullRequestUrl(project.contributionPullRequestUrl)
            .actorDefinitionId(project.contributionActorDefinitionId)
      }
      return detailsRead
    }

    private fun buildBaseActorDefinitionVersionInfo(
      actorDefinitionVersion: ActorDefinitionVersion,
      sourceDefinition: StandardSourceDefinition,
    ): BaseActorDefinitionVersionInfo =
      BaseActorDefinitionVersionInfo()
        .name(sourceDefinition.name)
        .dockerRepository(actorDefinitionVersion.dockerRepository)
        .dockerImageTag(actorDefinitionVersion.dockerImageTag)
        .actorDefinitionId(actorDefinitionVersion.actorDefinitionId)
        .icon(sourceDefinition.iconUrl)
        .documentationUrl(actorDefinitionVersion.documentationUrl)

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun builderProjectToDetailsRead(project: ConnectorBuilderProject): ConnectorBuilderProjectDetailsRead {
      val detailsRead = getProjectDetailsWithoutBaseAdvInfo(project)
      if (project.baseActorDefinitionVersionId != null) {
        val baseActorDefinitionVersion =
          actorDefinitionService.getActorDefinitionVersion(project.baseActorDefinitionVersionId)
        detailsRead.baseActorDefinitionVersionInfo(
          buildBaseActorDefinitionVersionInfo(
            baseActorDefinitionVersion,
            sourceService.getStandardSourceDefinition(baseActorDefinitionVersion.actorDefinitionId),
          ),
        )
      }
      return detailsRead
    }

    @Throws(IOException::class)
    private fun builderProjectsToDetailsReads(projects: List<ConnectorBuilderProject>): List<ConnectorBuilderProjectDetailsRead> {
      val baseActorDefinitionVersionIds =
        projects
          .stream()
          .map { obj: ConnectorBuilderProject -> obj.baseActorDefinitionVersionId }
          .distinct()
          .filter { obj: UUID? -> Objects.nonNull(obj) }
          .collect(Collectors.toList())

      val baseActorDefinitionVersions = actorDefinitionService.getActorDefinitionVersions(baseActorDefinitionVersionIds)

      val baseAdvIdToAdvMap =
        baseActorDefinitionVersions.stream().collect(
          Collectors.toMap(
            Function { obj: ActorDefinitionVersion -> obj.versionId },
            Function.identity(),
          ),
        )
      val standardSourceDefinitionsMap =
        sourceService
          .listStandardSourceDefinitions(false)
          .stream()
          .collect(
            Collectors.toMap(
              Function { obj: StandardSourceDefinition -> obj.sourceDefinitionId },
              Function.identity(),
            ),
          )

      val baseAdvIdToAssociatedSourceDefMap =
        baseActorDefinitionVersions
          .stream()
          .collect(
            Collectors.toMap(
              Function { obj: ActorDefinitionVersion -> obj.versionId },
              Function { actorDefinitionVersion: ActorDefinitionVersion ->
                standardSourceDefinitionsMap[actorDefinitionVersion.actorDefinitionId]
              },
            ),
          )

      return projects
        .stream()
        .map<ConnectorBuilderProjectDetailsRead> { project: ConnectorBuilderProject ->
          val detailsRead = getProjectDetailsWithoutBaseAdvInfo(project)
          val baseAdvId = project.baseActorDefinitionVersionId
          if (baseAdvId != null) {
            detailsRead
              .baseActorDefinitionVersionInfo(
                buildBaseActorDefinitionVersionInfo(
                  baseAdvIdToAdvMap[baseAdvId]!!,
                  baseAdvIdToAssociatedSourceDefMap[baseAdvId]!!,
                ),
              )
          }
          detailsRead
        }.collect(Collectors.toList<ConnectorBuilderProjectDetailsRead>())
    }

    private fun buildIdResponseFromId(
      projectId: UUID,
      workspaceId: UUID,
    ): ConnectorBuilderProjectIdWithWorkspaceId = ConnectorBuilderProjectIdWithWorkspaceId().workspaceId(workspaceId).builderProjectId(projectId)

    @Throws(ConfigNotFoundException::class, IOException::class)
    private fun validateProjectUnderRightWorkspace(
      projectId: UUID,
      workspaceId: UUID,
    ) {
      val project = connectorBuilderService.getConnectorBuilderProject(projectId, false)
      validateProjectUnderRightWorkspace(project, workspaceId)
    }

    @Throws(ConfigNotFoundException::class)
    private fun validateProjectUnderRightWorkspace(
      project: ConnectorBuilderProject,
      workspaceId: UUID,
    ) {
      val actualWorkspaceId = project.workspaceId
      if (actualWorkspaceId != workspaceId) {
        throw ConfigNotFoundException(ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT, project.builderProjectId.toString())
      }
    }

    @Throws(IOException::class)
    fun createConnectorBuilderProject(projectCreate: ConnectorBuilderProjectWithWorkspaceId): ConnectorBuilderProjectIdWithWorkspaceId {
      val id = uuidSupplier.get()

      connectorBuilderService.writeBuilderProjectDraft(
        id,
        projectCreate.workspaceId,
        projectCreate.builderProject.name,
        ObjectMapper().valueToTree(projectCreate.builderProject.draftManifest),
        projectCreate.builderProject.componentsFileContent,
        projectCreate.builderProject.baseActorDefinitionVersionId,
        projectCreate.builderProject.contributionPullRequestUrl,
        projectCreate.builderProject.contributionActorDefinitionId,
      )

      return buildIdResponseFromId(id, projectCreate.workspaceId)
    }

    /**
     * Apply defaults from the persisted project to the update. These fields will only be passed when we
     * are trying to set them (patch-style), and cannot be currently un-set. Therefore, grab the
     * persisted value if the update does not contain it, since all fields are passed into the update
     * method.
     */
    private fun applyPatchDefaultsFromDb(
      projectDetailsUpdate: ConnectorBuilderProjectDetails,
      persistedProject: ConnectorBuilderProject,
    ): ConnectorBuilderProjectDetails {
      if (projectDetailsUpdate.baseActorDefinitionVersionId == null) {
        projectDetailsUpdate.baseActorDefinitionVersionId = persistedProject.baseActorDefinitionVersionId
      }
      if (projectDetailsUpdate.contributionPullRequestUrl == null) {
        projectDetailsUpdate.contributionPullRequestUrl = persistedProject.contributionPullRequestUrl
      }
      if (projectDetailsUpdate.contributionActorDefinitionId == null) {
        projectDetailsUpdate.contributionActorDefinitionId = persistedProject.contributionActorDefinitionId
      }
      return projectDetailsUpdate
    }

    @Throws(ConfigNotFoundException::class, IOException::class)
    fun updateConnectorBuilderProject(projectUpdate: ExistingConnectorBuilderProjectWithWorkspaceId) {
      val connectorBuilderProject =
        connectorBuilderService.getConnectorBuilderProject(projectUpdate.builderProjectId, false)
      validateProjectUnderRightWorkspace(connectorBuilderProject, projectUpdate.workspaceId)

      val projectDetailsUpdate = applyPatchDefaultsFromDb(projectUpdate.builderProject, connectorBuilderProject)

      buildProjectUpdater.persistBuilderProjectUpdate(projectUpdate.builderProject(projectDetailsUpdate))
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun deleteConnectorBuilderProject(projectDelete: ConnectorBuilderProjectIdWithWorkspaceId) {
      validateProjectUnderRightWorkspace(projectDelete.builderProjectId, projectDelete.workspaceId)
      connectorBuilderService.deleteBuilderProject(projectDelete.builderProjectId)
    }

    @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
    fun getConnectorBuilderProjectWithManifest(request: ConnectorBuilderProjectIdWithWorkspaceId): ConnectorBuilderProjectRead {
      if (request.version != null) {
        validateProjectUnderRightWorkspace(request.builderProjectId, request.workspaceId)
        return buildConnectorBuilderProjectVersionManifestRead(
          connectorBuilderService.getVersionedConnectorBuilderProject(request.builderProjectId, request.version),
        )
      }

      return getWithManifestWithoutVersion(request)
    }

    @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
    private fun getWithManifestWithoutVersion(request: ConnectorBuilderProjectIdWithWorkspaceId): ConnectorBuilderProjectRead {
      val project = connectorBuilderService.getConnectorBuilderProject(request.builderProjectId, true)
      validateProjectUnderRightWorkspace(project, request.workspaceId)
      val response = ConnectorBuilderProjectRead().builderProject(builderProjectToDetailsRead(project))

      if (project.manifestDraft != null) {
        response.declarativeManifest =
          DeclarativeManifestRead()
            .manifest(project.manifestDraft)
            .isDraft(true)
        response.testingValues = maskSecrets(project.testingValues, project.manifestDraft)
      } else if (project.actorDefinitionId != null) {
        val declarativeManifest =
          connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
            project.actorDefinitionId,
          )
        response.declarativeManifest =
          DeclarativeManifestRead()
            .isDraft(false)
            .manifest(declarativeManifest.manifest)
            .version(declarativeManifest.version)
            .description(declarativeManifest.description)
        response.builderProject.componentsFileContent = declarativeManifest.componentsFileContent
        response.testingValues = maskSecrets(project.testingValues, declarativeManifest.manifest)
      }
      return response
    }

    private fun buildConnectorBuilderProjectVersionManifestRead(project: ConnectorBuilderProjectVersionedManifest): ConnectorBuilderProjectRead =
      ConnectorBuilderProjectRead()
        .builderProject(
          ConnectorBuilderProjectDetailsRead()
            .builderProjectId(project.builderProjectId)
            .name(project.name)
            .hasDraft(project.hasDraft)
            .activeDeclarativeManifestVersion(project.activeDeclarativeManifestVersion)
            .sourceDefinitionId(project.sourceDefinitionId)
            .componentsFileContent(project.componentsFileContent),
        ).declarativeManifest(
          DeclarativeManifestRead()
            .isDraft(false)
            .manifest(project.manifest)
            .version(project.manifestVersion)
            .description(project.manifestDescription),
        ).testingValues(maskSecrets(project.testingValues, project.manifest))

    private fun maskSecrets(
      testingValues: JsonNode?,
      manifest: JsonNode,
    ): JsonNode? {
      val spec = manifest[SPEC_FIELD]
      if (spec != null) {
        val connectionSpecification = spec[CONNECTION_SPECIFICATION_FIELD]
        if (connectionSpecification != null && testingValues != null) {
          return secretsProcessor.prepareSecretsForOutput(testingValues, connectionSpecification)
        }
      }
      return testingValues
    }

    @Throws(IOException::class)
    fun listConnectorBuilderProjects(workspaceIdRequestBody: WorkspaceIdRequestBody): ConnectorBuilderProjectReadList {
      val projects =
        connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceIdRequestBody.workspaceId)

      return ConnectorBuilderProjectReadList().projects(builderProjectsToDetailsReads(projects.toList()))
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun publishConnectorBuilderProject(connectorBuilderPublishRequestBody: ConnectorBuilderPublishRequestBody): SourceDefinitionIdBody {
      validateProjectUnderRightWorkspace(connectorBuilderPublishRequestBody.builderProjectId, connectorBuilderPublishRequestBody.workspaceId)
      val manifest = connectorBuilderPublishRequestBody.initialDeclarativeManifest.manifest
      val spec = connectorBuilderPublishRequestBody.initialDeclarativeManifest.spec
      val componentsFileContent = connectorBuilderPublishRequestBody.componentsFileContent

      manifestInjector.addInjectedDeclarativeManifest(spec)
      val actorDefinitionId =
        createActorDefinition(
          connectorBuilderPublishRequestBody.name,
          connectorBuilderPublishRequestBody.workspaceId,
          manifest,
          spec,
          componentsFileContent,
        )

      val declarativeManifest =
        DeclarativeManifest()
          .withActorDefinitionId(actorDefinitionId)
          .withVersion(connectorBuilderPublishRequestBody.initialDeclarativeManifest.version)
          .withDescription(connectorBuilderPublishRequestBody.initialDeclarativeManifest.description)
          .withManifest(manifest)
          .withSpec(spec)
          .withComponentsFileContent(componentsFileContent)

      connectorBuilderService.insertActiveDeclarativeManifest(declarativeManifest)
      connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(
        connectorBuilderPublishRequestBody.builderProjectId,
        actorDefinitionId,
      )
      connectorBuilderService.deleteBuilderProjectDraft(connectorBuilderPublishRequestBody.builderProjectId)

      return SourceDefinitionIdBody().sourceDefinitionId(actorDefinitionId)
    }

    @Throws(IOException::class)
    private fun createActorDefinition(
      name: String,
      workspaceId: UUID,
      manifest: JsonNode,
      spec: JsonNode,
      componentFileContent: String?,
    ): UUID {
      val connectorSpecification = manifestInjector.createDeclarativeManifestConnectorSpecification(spec)
      val actorDefinitionId = uuidSupplier.get()
      val source =
        StandardSourceDefinition()
          .withSourceDefinitionId(actorDefinitionId)
          .withName(name)
          .withSourceType(StandardSourceDefinition.SourceType.CUSTOM)
          .withTombstone(false)
          .withPublic(false)
          .withCustom(true)

      val defaultVersion =
        ActorDefinitionVersion()
          .withActorDefinitionId(actorDefinitionId)
          .withDockerImageTag(getImageVersionForManifest(manifest).imageVersion)
          .withDockerRepository(AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
          .withSpec(connectorSpecification)
          .withProtocolVersion(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
          .withReleaseStage(ReleaseStage.CUSTOM)
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withDocumentationUrl(connectorSpecification.documentationUrl.toString())

      // Scope connector to the organization if present, otherwise scope to the workspace.
      val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId)
      val scopeId = organizationId.orElse(workspaceId)
      val scopeType = if (organizationId.isPresent) ScopeType.ORGANIZATION else ScopeType.WORKSPACE
      sourceService.writeCustomConnectorMetadata(source, defaultVersion, scopeId, scopeType)

      val configInjectionsToCreate =
        manifestInjector.getManifestConnectorInjections(source.sourceDefinitionId, manifest, componentFileContent)
      connectorBuilderService.writeActorDefinitionConfigInjectionsForPath(configInjectionsToCreate)

      return source.sourceDefinitionId
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun updateConnectorBuilderProjectTestingValues(testingValuesUpdate: ConnectorBuilderProjectTestingValuesUpdate): JsonNode {
      try {
        validateProjectUnderRightWorkspace(testingValuesUpdate.builderProjectId, testingValuesUpdate.workspaceId)
        val project = connectorBuilderService.getConnectorBuilderProject(testingValuesUpdate.builderProjectId, false)
        val existingTestingValues = Optional.ofNullable(project.testingValues)

        val secretPersistenceConfig = getSecretPersistenceConfig(project.workspaceId)
        val existingHydratedTestingValues = getHydratedTestingValues(project, secretPersistenceConfig.orElse(null))

        val updatedTestingValues =
          if (existingHydratedTestingValues.isPresent) {
            secretsProcessor.copySecrets(
              existingHydratedTestingValues.get(),
              testingValuesUpdate.testingValues,
              testingValuesUpdate.spec,
            )
          } else {
            testingValuesUpdate.testingValues
          }

        val updatedTestingValuesWithSecretCoordinates =
          writeSecretsToSecretPersistence(
            existingTestingValues,
            updatedTestingValues,
            testingValuesUpdate.spec,
            project.workspaceId,
            secretPersistenceConfig,
          )

        connectorBuilderService.updateBuilderProjectTestingValues(testingValuesUpdate.builderProjectId, updatedTestingValuesWithSecretCoordinates)
        return secretsProcessor.prepareSecretsForOutput(updatedTestingValuesWithSecretCoordinates, testingValuesUpdate.spec)
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun readConnectorBuilderProjectStream(requestBody: ConnectorBuilderProjectStreamReadRequestBody): ConnectorBuilderProjectStreamRead {
      try {
        val project = connectorBuilderService.getConnectorBuilderProject(requestBody.builderProjectId, false)
        val secretPersistenceConfig = getSecretPersistenceConfig(project.workspaceId)
        val existingHydratedTestingValues =
          getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject())

        val streamReadRequestBody =
          StreamReadRequestBody(
            existingHydratedTestingValues,
            requestBody.manifest,
            requestBody.streamName,
            requestBody.customComponentsCode,
            requestBody.formGeneratedManifest,
            requestBody.builderProjectId.toString(),
            requestBody.recordLimit,
            requestBody.pageLimit,
            requestBody.sliceLimit,
            requestBody.state,
            requestBody.workspaceId.toString(),
          )
        val streamRead = connectorBuilderServerApiClient.readStream(streamReadRequestBody)

        val builderProjectStreamRead = convertStreamRead(streamRead)

        if (streamRead.latestConfigUpdate != null) {
          val spec = requestBody.manifest[SPEC_FIELD][CONNECTION_SPECIFICATION_FIELD]
          val updatedTestingValuesWithSecretCoordinates =
            writeSecretsToSecretPersistence(
              Optional.ofNullable(project.testingValues),
              Jsons.convertValue(streamRead.latestConfigUpdate, JsonNode::class.java),
              spec,
              project.workspaceId,
              secretPersistenceConfig,
            )
          connectorBuilderService.updateBuilderProjectTestingValues(project.builderProjectId, updatedTestingValuesWithSecretCoordinates)
          val updatedTestingValuesWithObfuscatedSecrets =
            secretsProcessor.prepareSecretsForOutput(updatedTestingValuesWithSecretCoordinates, spec)
          builderProjectStreamRead.latestConfigUpdate = updatedTestingValuesWithObfuscatedSecrets
        }

        return builderProjectStreamRead
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class)
    fun fullResolveManifestBuilderProject(requestBody: ConnectorBuilderProjectFullResolveRequestBody): ConnectorBuilderProjectFullResolveResponse {
      val project = connectorBuilderService.getConnectorBuilderProject(requestBody.builderProjectId, false)
      val secretPersistenceConfig = getSecretPersistenceConfig(project.workspaceId)
      val existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject())

      val fullResolveManifestRequestBody =
        FullResolveManifestRequestBody(
          existingHydratedTestingValues,
          requestBody.manifest,
          requestBody.streamLimit,
          requestBody.builderProjectId.toString(),
          requestBody.workspaceId.toString(),
        )
      val resolveManifest = connectorBuilderServerApiClient.fullResolveManifest(fullResolveManifestRequestBody)
      val builderProjectResolveManifest = convertResolveManifest(resolveManifest)
      return builderProjectResolveManifest
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getConnectorBuilderProjectForDefinitionId(requestBody: BuilderProjectForDefinitionRequestBody): BuilderProjectForDefinitionResponse {
      val builderProjectId: Optional<UUID> =
        connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(requestBody.actorDefinitionId)

      var workspaceId: Optional<UUID> = Optional.empty()
      if (builderProjectId.isPresent) {
        workspaceId =
          Optional
            .ofNullable(
              connectorBuilderService
                .getConnectorBuilderProject(builderProjectId.get(), false),
            ).map { obj: ConnectorBuilderProject -> obj.workspaceId }
      }

      return BuilderProjectForDefinitionResponse()
        .builderProjectId(builderProjectId.orElse(null))
        .workspaceId(workspaceId.orElse(null))
    }

    /**
     * Converts the provided `StreamRead` object into a `ConnectorBuilderProjectStreamRead`
     * object.
     *
     *
     * This method maps various properties from the `streamRead` instance including logs, slices,
     * auxiliary requests, inferred schema, and inferred datetime formats. It also transfers the test
     * read limit flag.
     *
     * @param streamRead the `StreamRead` instance containing the original data to be converted.
     * @return a new `ConnectorBuilderProjectStreamRead` instance populated with the mapped values
     * from `streamRead`.
     */
    private fun convertStreamRead(streamRead: StreamRead): ConnectorBuilderProjectStreamRead =
      ConnectorBuilderProjectStreamRead()
        .logs(mapStreamReadLogs(streamRead))
        .slices(mapStreamReadSlices(streamRead))
        .testReadLimitReached(streamRead.testReadLimitReached)
        .auxiliaryRequests(mapGlobalAuxiliaryRequests(streamRead))
        .inferredSchema(streamRead.inferredSchema)
        .inferredDatetimeFormats(streamRead.inferredDatetimeFormats)

    /**
     * Converts the provided `ResolveManifest` object into a
     * `ConnectorBuilderProjectFullResolveResponse` object.
     *
     *
     * This method maps manifest property from the `resolveManifest` instance.
     *
     * @param resolveManifest the `ResolveManifest` instance containing the original data to be
     * converted.
     * @return a new `ConnectorBuilderProjectFullResolveResponse` instance populated with the
     * mapped value from `resolveManifest`.
     */
    private fun convertResolveManifest(resolveManifest: ResolveManifest): ConnectorBuilderProjectFullResolveResponse =
      ConnectorBuilderProjectFullResolveResponse()
        .manifest(resolveManifest.manifest)

    /**
     * Converts the logs contained within a `StreamRead` object into a list of
     * `ConnectorBuilderProjectStreamReadLogsInner` instances.
     *
     *
     *
     * Each log entry in the provided `streamRead` is transformed by mapping its message, level
     * (converted via `LevelEnum.fromString`), internal message, and stacktrace.
     *
     *
     * @param streamRead the `StreamRead` instance containing the logs to be mapped
     * @return a list of `ConnectorBuilderProjectStreamReadLogsInner` objects representing the
     * mapped log entries
     */
    private fun mapStreamReadLogs(streamRead: StreamRead): List<ConnectorBuilderProjectStreamReadLogsInner> =
      streamRead.logs
        .stream()
        .map { log: StreamReadLogsInner ->
          ConnectorBuilderProjectStreamReadLogsInner()
            .message(log.message)
            .level(ConnectorBuilderProjectStreamReadLogsInner.LevelEnum.fromString(log.level.value))
            .internalMessage(log.internalMessage)
            .stacktrace(log.stacktrace)
        }.toList()

    /**
     * Maps the slices from the given StreamRead instance into a list of
     * ConnectorBuilderProjectStreamReadSlicesInner objects.
     *
     *
     *
     * This method iterates through each slice in the provided StreamRead, creating a corresponding
     * ConnectorBuilderProjectStreamReadSlicesInner for each slice. For each slice, it:
     *
     *  * Sets the slice descriptor.
     *  * Ensures the state list is not empty; otherwise, it uses an empty list.
     *  * Converts each page by mapping records and transforming both the HTTP request and
     * response.
     *  * Processes auxiliary requests specific to the slice.
     *
     *
     * @param streamRead the StreamRead instance containing slices to be mapped.
     * @return a list of ConnectorBuilderProjectStreamReadSlicesInner objects constructed from the
     * StreamRead slices.
     */
    private fun mapStreamReadSlices(streamRead: StreamRead): List<ConnectorBuilderProjectStreamReadSlicesInner> =
      streamRead.slices
        .stream()
        .map { slice: StreamReadSlicesInner ->
          ConnectorBuilderProjectStreamReadSlicesInner()
            .sliceDescriptor(slice.sliceDescriptor)
            .state(if (CollectionUtils.isNotEmpty(slice.state)) slice.state else listOf())
            .pages(
              slice.pages
                .stream()
                .map { page: StreamReadSlicesInnerPagesInner ->
                  ConnectorBuilderProjectStreamReadSlicesInnerPagesInner()
                    .records(page.records)
                    .request(convertHttpRequest(page.request))
                    .response(convertHttpResponse(page.response))
                }.toList(),
            ).auxiliaryRequests(mapSliceAuxiliaryRequests(slice))
        }.toList()

    /**
     * Converts a list of AuxiliaryRequest objects into a list of ConnectorBuilderAuxiliaryRequest
     * objects.
     *
     *
     *
     * This method first checks if the provided list is not empty. If it is not empty, it streams over
     * the list, mapping each AuxiliaryRequest into a ConnectorBuilderAuxiliaryRequest by copying the
     * description, title, and converting the HTTP request and response fields as well as mapping the
     * type using the TypeEnum. If the input list is empty, it returns an empty list.
     *
     * @param auxiliaryRequests the list of AuxiliaryRequest objects to be converted
     * @return a list of converted ConnectorBuilderAuxiliaryRequest objects, or an empty list if the
     * input is empty
     */
    private fun mapAuxiliaryRequests(auxiliaryRequests: List<AuxiliaryRequest?>?): List<ConnectorBuilderAuxiliaryRequest> =
      if (CollectionUtils.isNotEmpty(auxiliaryRequests)) {
        auxiliaryRequests!!
          .stream()
          .map { auxRequest: AuxiliaryRequest? ->
            ConnectorBuilderAuxiliaryRequest()
              .description(auxRequest!!.description)
              .request(convertHttpRequest(auxRequest.request))
              .response(convertHttpResponse(auxRequest.response))
              .title(auxRequest.title)
              .type(ConnectorBuilderAuxiliaryRequest.TypeEnum.fromString(auxRequest.type.value))
          }.toList()
      } else {
        listOf()
      }

    /**
     * Maps the global auxiliary requests from the provided StreamRead instance.
     *
     *
     *
     * This method retrieves the auxiliary requests from the StreamRead object and delegates the mapping
     * to the mapAuxiliaryRequests method.
     *
     * @param streamRead the StreamRead instance that contains the auxiliary requests
     * @return a list of ConnectorBuilderAuxiliaryRequest objects generated from the auxiliary requests
     */
    private fun mapGlobalAuxiliaryRequests(streamRead: StreamRead): List<ConnectorBuilderAuxiliaryRequest> =
      mapAuxiliaryRequests(streamRead.auxiliaryRequests)

    /**
     * Maps the auxiliary requests from the given slice into a list of ConnectorBuilderAuxiliaryRequest
     * instances.
     *
     * @param slice the input slice containing auxiliary request data
     * @return a list of ConnectorBuilderAuxiliaryRequest objects mapped from the slice's auxiliary
     * requests
     */
    private fun mapSliceAuxiliaryRequests(slice: StreamReadSlicesInner): List<ConnectorBuilderAuxiliaryRequest> =
      mapAuxiliaryRequests(slice.auxiliaryRequests)

    private fun convertHttpRequest(
      @Nullable request: HttpRequest?,
    ): ConnectorBuilderHttpRequest? =
      if (request != null) {
        ConnectorBuilderHttpRequest()
          .url(request.url)
          .httpMethod(ConnectorBuilderHttpRequest.HttpMethodEnum.fromString(request.httpMethod.value))
          .body(request.body)
          .headers(request.headers)
      } else {
        null
      }

    private fun convertHttpResponse(response: HttpResponse?): ConnectorBuilderHttpResponse? =
      if (response != null) {
        ConnectorBuilderHttpResponse()
          .status(response.status)
          .body(response.body)
          .headers(response.headers)
      } else {
        null
      }

    @Throws(JsonValidationException::class)
    private fun writeSecretsToSecretPersistence(
      existingTestingValues: Optional<JsonNode>,
      updatedTestingValues: JsonNode,
      spec: JsonNode,
      workspaceId: UUID,
      secretPersistenceConfig: Optional<SecretPersistenceConfig>,
    ): JsonNode {
      val secretPersistence =
        secretPersistenceConfig
          .map { c: SecretPersistenceConfig? ->
            RuntimeSecretPersistence(
              c!!,
              metricClient,
            )
          }.orElse(null)
      if (existingTestingValues.isPresent) {
        return secretsRepositoryWriter.updateFromConfigLegacy(
          workspaceId,
          existingTestingValues.get(),
          updatedTestingValues,
          spec,
          secretPersistence,
        )
      }
      return secretsRepositoryWriter.createFromConfigLegacy(workspaceId, updatedTestingValues, spec, secretPersistence)
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    private fun getSecretPersistenceConfig(workspaceId: UUID): Optional<SecretPersistenceConfig> {
      try {
        val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId)
        return if (organizationId.isPresent &&
          featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId.get()))
        ) {
          Optional.of(secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.get()))
        } else {
          Optional.empty()
        }
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    private fun getHydratedTestingValues(
      project: ConnectorBuilderProject,
      @Nullable secretPersistenceConfig: SecretPersistenceConfig?,
    ): Optional<JsonNode> {
      val testingValues = Optional.ofNullable(project.testingValues)
      val secretPersistenceConfigOptional = Optional.ofNullable(secretPersistenceConfig)

      return if (testingValues.isPresent) {
        if (secretPersistenceConfigOptional.isPresent) {
          Optional.ofNullable(
            secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
              testingValues.get(),
              RuntimeSecretPersistence(secretPersistenceConfigOptional.get(), metricClient),
            ),
          )
        } else {
          Optional.ofNullable(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(project.testingValues))
        }
      } else {
        Optional.empty()
      }
    }

    fun getDeclarativeManifestBaseImage(declarativeManifestRequestBody: DeclarativeManifestRequestBody): DeclarativeManifestBaseImageRead {
      val declarativeManifest = declarativeManifestRequestBody.manifest
      val declarativeManifestImageVersion = getImageVersionForManifest(declarativeManifest)
      val baseImage =
        String.format(
          "docker.io/%s:%s@%s",
          AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE,
          declarativeManifestImageVersion.imageVersion,
          declarativeManifestImageVersion.imageSha,
        )
      return DeclarativeManifestBaseImageRead().baseImage(baseImage)
    }

    private fun getImageVersionForManifest(declarativeManifest: JsonNode): DeclarativeManifestImageVersion {
      val manifestVersion = manifestInjector.getCdkVersion(declarativeManifest)
      return declarativeManifestImageVersionService
        .getDeclarativeManifestImageVersionByMajorVersion(manifestVersion.getMajorVersion()!!.toInt())
    }

    @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
    fun createForkedConnectorBuilderProject(requestBody: ConnectorBuilderProjectForkRequestBody): ConnectorBuilderProjectIdWithWorkspaceId {
      val sourceDefinition = sourceService.getStandardSourceDefinition(requestBody.baseActorDefinitionId)
      val defaultVersion = actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId)
      val manifest =
        remoteDefinitionsProvider
          .getConnectorManifest(defaultVersion.dockerRepository, defaultVersion.dockerImageTag)
          .orElseGet {
            val errorMessage =
              "Could not fork connector: no manifest file available " +
                "for ${defaultVersion.dockerRepository}:${defaultVersion.dockerImageTag}"
            log.error(errorMessage)
            throw NotFoundException(errorMessage)
          }

      val customComponentsContent =
        remoteDefinitionsProvider
          .getConnectorCustomComponents(defaultVersion.dockerRepository, defaultVersion.dockerImageTag)
          .orElse(null)

      val projectDetails =
        ConnectorBuilderProjectDetails()
          .name(sourceDefinition.name)
          .baseActorDefinitionVersionId(defaultVersion.versionId)
          .draftManifest(manifest)
          .componentsFileContent(customComponentsContent)
      return createConnectorBuilderProject(
        ConnectorBuilderProjectWithWorkspaceId().workspaceId(requestBody.workspaceId).builderProject(projectDetails),
      )
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun getConnectorBuilderProjectOAuthConsent(requestBody: BuilderProjectOauthConsentRequest): OAuthConsentRead {
      val project = connectorBuilderService.getConnectorBuilderProject(requestBody.builderProjectId, true)
      val spec =
        Jsons.`object`(
          project.manifestDraft["spec"],
          ConnectorSpecification::class.java,
        )

      val secretPersistenceConfig = getSecretPersistenceConfig(project.workspaceId)
      val existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject())

      val traceTags =
        Map.of<String?, Any?>(
          WORKSPACE_ID_KEY,
          requestBody.workspaceId,
          CONNECTOR_BUILDER_PROJECT_ID_KEY,
          requestBody.builderProjectId,
        )
      addTagsToTrace(traceTags)
      addTagsToRootSpan(traceTags)

      val advancedAuthNull = spec.advancedAuth == null
      val oauthSpecNull = spec.advancedAuth?.oauthConfigSpecification == null

      if (advancedAuthNull || oauthSpecNull) {
        val missingFields =
          buildList {
            if (advancedAuthNull) add("advancedAuth")
            if (oauthSpecNull) add("advancedAuth.oauthConfigSpecification")
          }.joinToString(", ")

        throw FailedPreconditionProblem(
          data =
            FailedPreconditionData().failedPreconditionDetail(
              "Cannot fetch a consent URL because the following required fields are null in the connector spec: $missingFields.",
            ),
        )
      }

      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthFlowImplementation = oAuthImplementationFactory.createDeclarativeOAuthImplementation(spec)
      return OAuthConsentRead().consentUrl(
        oAuthFlowImplementation.getSourceConsentUrl(
          requestBody.workspaceId,
          null,
          requestBody.redirectUrl,
          existingHydratedTestingValues,
          oauthConfigSpecification,
          existingHydratedTestingValues,
        ),
      )
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun completeConnectorBuilderProjectOAuth(requestBody: CompleteConnectorBuilderProjectOauthRequest): CompleteOAuthResponse {
      val project = connectorBuilderService.getConnectorBuilderProject(requestBody.builderProjectId, true)
      val spec =
        Jsons.`object`(
          project.manifestDraft["spec"],
          ConnectorSpecification::class.java,
        )

      val secretPersistenceConfig = getSecretPersistenceConfig(project.workspaceId)
      val existingHydratedTestingValues =
        getHydratedTestingValues(project, secretPersistenceConfig.orElse(null)).orElse(Jsons.emptyObject())

      val traceTags =
        Map.of<String?, Any?>(
          WORKSPACE_ID_KEY,
          requestBody.workspaceId,
          CONNECTOR_BUILDER_PROJECT_ID_KEY,
          requestBody.builderProjectId,
        )
      addTagsToTrace(traceTags)
      addTagsToRootSpan(traceTags)

      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthFlowImplementation = oAuthImplementationFactory.createDeclarativeOAuthImplementation(spec)
      val result =
        oAuthFlowImplementation.completeSourceOAuth(
          requestBody.workspaceId,
          null,
          requestBody.queryParams,
          requestBody.redirectUrl,
          existingHydratedTestingValues,
          oauthConfigSpecification,
          existingHydratedTestingValues,
        )

      return mapToCompleteOAuthResponse(result)
    }

    companion object {
      private val log = KotlinLogging.logger {}
      const val SPEC_FIELD: String = "spec"
      const val CONNECTION_SPECIFICATION_FIELD: String = "connection_specification"
    }
  }
