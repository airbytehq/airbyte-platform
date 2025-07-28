/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DeclarativeManifestVersionRead
import io.airbyte.api.model.generated.DeclarativeManifestsReadList
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DeclarativeManifestImageVersionService
import io.airbyte.data.services.WorkspaceService
import jakarta.inject.Inject
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors

/**
 * DeclarativeSourceDefinitionsHandler. Javadocs suppressed because api docs should be used as
 * source of truth.
 */
@Singleton
open class DeclarativeSourceDefinitionsHandler
  @Inject
  constructor(
    private val declarativeManifestImageVersionService: DeclarativeManifestImageVersionService,
    private val connectorBuilderService: ConnectorBuilderService,
    private val workspaceService: WorkspaceService,
    private val manifestInjector: DeclarativeSourceManifestInjector,
    private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
  ) {
    @Throws(IOException::class)
    fun createDeclarativeSourceDefinitionManifest(requestBody: DeclarativeSourceDefinitionCreateManifestRequestBody) {
      validateAccessToSource(requestBody.sourceDefinitionId, requestBody.workspaceId)

      val existingVersions = fetchAvailableManifestVersions(requestBody.sourceDefinitionId)
      val version = requestBody.declarativeManifest.version
      val manifest = requestBody.declarativeManifest.manifest
      val componentFileContent = requestBody.componentsFileContent

      if (existingVersions.isEmpty()) {
        throw SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.sourceDefinitionId),
        )
      } else if (existingVersions.contains(version)) {
        throw ValueConflictKnownException(String.format("Version '%s' for source %s already exists", version, requestBody.sourceDefinitionId))
      }

      val spec = requestBody.declarativeManifest.spec
      manifestInjector.addInjectedDeclarativeManifest(spec)
      val declarativeManifest =
        DeclarativeManifest()
          .withActorDefinitionId(requestBody.sourceDefinitionId)
          .withVersion(version)
          .withDescription(requestBody.declarativeManifest.description)
          .withManifest(manifest)
          .withSpec(spec)
          .withComponentsFileContent(componentFileContent)

      if (requestBody.setAsActiveManifest) {
        val configInjectionsToCreate =
          manifestInjector.getManifestConnectorInjections(
            requestBody.sourceDefinitionId,
            manifest,
            componentFileContent,
          )

        connectorBuilderService.createDeclarativeManifestAsActiveVersion(
          declarativeManifest,
          configInjectionsToCreate,
          manifestInjector.createDeclarativeManifestConnectorSpecification(spec),
          getImageVersionForManifest(declarativeManifest).imageVersion,
        )
      } else {
        connectorBuilderService.insertDeclarativeManifest(declarativeManifest)
      }
      connectorBuilderService.deleteManifestDraftForActorDefinition(requestBody.sourceDefinitionId, requestBody.workspaceId)
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun updateDeclarativeManifestVersion(requestBody: UpdateActiveManifestRequestBody) {
      val sourceDefinitionId = requestBody.sourceDefinitionId
      val workspaceId = requestBody.workspaceId
      val version = requestBody.version

      validateAccessToSource(sourceDefinitionId, workspaceId)
      val existingVersions = fetchAvailableManifestVersions(sourceDefinitionId)
      if (existingVersions.isEmpty()) {
        throw SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", sourceDefinitionId),
        )
      }

      val declarativeManifest =
        connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
          sourceDefinitionId,
          version,
        )

      val imageVersionForManifest = getImageVersionForManifest(declarativeManifest).imageVersion
      val isNewConnectorVersionSupported =
        airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(imageVersionForManifest)

      if (!isNewConnectorVersionSupported.isValid) {
        val message =
          if (isNewConnectorVersionSupported.message != null) {
            isNewConnectorVersionSupported.message
          } else {
            String.format(
              "Declarative manifest can't be updated to version %s because the version " +
                "is not supported by current platform version",
              imageVersionForManifest,
            )
          }
        throw BadRequestProblem(message, ProblemMessageData().message(message))
      }

      val configInjectionsToCreate =
        manifestInjector.getManifestConnectorInjections(
          sourceDefinitionId,
          declarativeManifest.manifest,
          declarativeManifest.componentsFileContent,
        )

      connectorBuilderService.setDeclarativeSourceActiveVersion(
        sourceDefinitionId,
        declarativeManifest.version,
        configInjectionsToCreate,
        manifestInjector.createDeclarativeManifestConnectorSpecification(declarativeManifest.spec),
        imageVersionForManifest,
      )
    }

    @Throws(IOException::class)
    private fun fetchAvailableManifestVersions(sourceDefinitionId: UUID): Collection<Long> =
      connectorBuilderService
        .getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId)
        .map { obj: DeclarativeManifest -> obj.version }
        .collect(Collectors.toSet())

    @Throws(IOException::class)
    private fun validateAccessToSource(
      actorDefinitionId: UUID,
      workspaceId: UUID,
    ) {
      if (!workspaceService.workspaceCanUseCustomDefinition(actorDefinitionId, workspaceId)) {
        throw DeclarativeSourceNotFoundException(
          String.format("Can't find source definition id `%s` in workspace %s", actorDefinitionId, workspaceId),
        )
      }
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun listManifestVersions(requestBody: ListDeclarativeManifestsRequestBody): DeclarativeManifestsReadList {
      validateAccessToSource(requestBody.sourceDefinitionId, requestBody.workspaceId)
      val existingVersions =
        connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(
          requestBody.sourceDefinitionId,
        )
      val activeVersion =
        connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(requestBody.sourceDefinitionId)

      return DeclarativeManifestsReadList().manifestVersions(
        existingVersions
          .map { manifest: DeclarativeManifest ->
            DeclarativeManifestVersionRead()
              .description(manifest.description)
              .version(manifest.version)
              .isActive(manifest.version == activeVersion.version)
          }.sorted(Comparator.comparingLong { obj: DeclarativeManifestVersionRead? -> obj!!.version })
          .collect(Collectors.toList<@Valid DeclarativeManifestVersionRead?>()),
      )
    }

    private fun getImageVersionForManifest(declarativeManifest: DeclarativeManifest): DeclarativeManifestImageVersion {
      val manifestVersion = manifestInjector.getCdkVersion(declarativeManifest.manifest)
      return declarativeManifestImageVersionService
        .getDeclarativeManifestImageVersionByMajorVersion(manifestVersion.getMajorVersion()!!.toInt())
    }
  }
