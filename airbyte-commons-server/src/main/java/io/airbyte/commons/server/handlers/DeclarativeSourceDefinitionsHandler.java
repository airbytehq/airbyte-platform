/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.DeclarativeManifestVersionRead;
import io.airbyte.api.model.generated.DeclarativeManifestsReadList;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody;
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody;
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.BadRequestProblem;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.commons.version.Version;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DeclarativeManifestImageVersionService;
import io.airbyte.data.services.WorkspaceService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DeclarativeSourceDefinitionsHandler. Javadocs suppressed because api docs should be used as
 * source of truth.
 */
@Singleton
public class DeclarativeSourceDefinitionsHandler {

  private final DeclarativeManifestImageVersionService declarativeManifestImageVersionService;
  private final ConnectorBuilderService connectorBuilderService;
  private final WorkspaceService workspaceService;
  private final DeclarativeSourceManifestInjector manifestInjector;
  private final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator;

  @Inject
  public DeclarativeSourceDefinitionsHandler(final DeclarativeManifestImageVersionService declarativeManifestImageVersionService,
                                             final ConnectorBuilderService connectorBuilderService,
                                             final WorkspaceService workspaceService,
                                             final DeclarativeSourceManifestInjector manifestInjector,
                                             final AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator) {
    this.declarativeManifestImageVersionService = declarativeManifestImageVersionService;
    this.connectorBuilderService = connectorBuilderService;
    this.workspaceService = workspaceService;
    this.manifestInjector = manifestInjector;
    this.airbyteCompatibleConnectorsValidator = airbyteCompatibleConnectorsValidator;
  }

  public void createDeclarativeSourceDefinitionManifest(final DeclarativeSourceDefinitionCreateManifestRequestBody requestBody) throws IOException {
    validateAccessToSource(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());

    final Collection<Long> existingVersions = fetchAvailableManifestVersions(requestBody.getSourceDefinitionId());
    final long version = requestBody.getDeclarativeManifest().getVersion();
    if (existingVersions.isEmpty()) {
      throw new SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.getSourceDefinitionId()));
    } else if (existingVersions.contains(version)) {
      throw new ValueConflictKnownException(String.format("Version '%s' for source %s already exists", version, requestBody.getSourceDefinitionId()));
    }

    final JsonNode spec = requestBody.getDeclarativeManifest().getSpec();
    manifestInjector.addInjectedDeclarativeManifest(spec);
    final DeclarativeManifest declarativeManifest = new DeclarativeManifest()
        .withActorDefinitionId(requestBody.getSourceDefinitionId())
        .withVersion(version)
        .withDescription(requestBody.getDeclarativeManifest().getDescription())
        .withManifest(requestBody.getDeclarativeManifest().getManifest())
        .withSpec(spec);
    if (requestBody.getSetAsActiveManifest()) {
      connectorBuilderService.createDeclarativeManifestAsActiveVersion(declarativeManifest,
          manifestInjector.createConfigInjection(requestBody.getSourceDefinitionId(), declarativeManifest.getManifest()),
          manifestInjector.createDeclarativeManifestConnectorSpecification(spec), getImageVersionForManifest(declarativeManifest).getImageVersion());
    } else {
      connectorBuilderService.insertDeclarativeManifest(declarativeManifest);
    }
    connectorBuilderService.deleteManifestDraftForActorDefinition(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
  }

  public void updateDeclarativeManifestVersion(final UpdateActiveManifestRequestBody requestBody) throws IOException, ConfigNotFoundException {
    validateAccessToSource(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
    final Collection<Long> existingVersions = fetchAvailableManifestVersions(requestBody.getSourceDefinitionId());
    if (existingVersions.isEmpty()) {
      throw new SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.getSourceDefinitionId()));
    }
    final DeclarativeManifest declarativeManifest = connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(
        requestBody.getSourceDefinitionId(), requestBody.getVersion());
    final String imageVersionForManifest = getImageVersionForManifest(declarativeManifest).getImageVersion();
    final ConnectorPlatformCompatibilityValidationResult isNewConnectorVersionSupported =
        airbyteCompatibleConnectorsValidator.validateDeclarativeManifest(imageVersionForManifest);
    if (!isNewConnectorVersionSupported.isValid()) {
      final String message = isNewConnectorVersionSupported.getMessage() != null ? isNewConnectorVersionSupported.getMessage()
          : String.format("Declarative manifest can't be updated to version %s because the version "
              + "is not supported by current platform version",
              imageVersionForManifest);
      throw new BadRequestProblem(message, new ProblemMessageData().message(message));
    }
    connectorBuilderService.setDeclarativeSourceActiveVersion(requestBody.getSourceDefinitionId(),
        declarativeManifest.getVersion(),
        manifestInjector.createConfigInjection(declarativeManifest.getActorDefinitionId(), declarativeManifest.getManifest()),
        manifestInjector.createDeclarativeManifestConnectorSpecification(declarativeManifest.getSpec()),
        imageVersionForManifest);
  }

  private Collection<Long> fetchAvailableManifestVersions(final UUID sourceDefinitionId) throws IOException {
    return connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId)
        .map(DeclarativeManifest::getVersion)
        .collect(Collectors.toSet());
  }

  private void validateAccessToSource(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    if (!workspaceService.workspaceCanUseCustomDefinition(actorDefinitionId, workspaceId)) {
      throw new DeclarativeSourceNotFoundException(
          String.format("Can't find source definition id `%s` in workspace %s", actorDefinitionId, workspaceId));
    }
  }

  public DeclarativeManifestsReadList listManifestVersions(
                                                           final ListDeclarativeManifestsRequestBody requestBody)
      throws IOException, ConfigNotFoundException {
    validateAccessToSource(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
    final Stream<DeclarativeManifest> existingVersions = connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(
        requestBody.getSourceDefinitionId());
    final DeclarativeManifest activeVersion =
        connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(requestBody.getSourceDefinitionId());

    return new DeclarativeManifestsReadList().manifestVersions(existingVersions
        .map(manifest -> new DeclarativeManifestVersionRead().description(
            manifest.getDescription()).version(manifest.getVersion()).isActive(manifest.getVersion().equals(activeVersion.getVersion())))
        .sorted(Comparator.comparingLong(DeclarativeManifestVersionRead::getVersion)).collect(Collectors.toList()));
  }

  private DeclarativeManifestImageVersion getImageVersionForManifest(final DeclarativeManifest declarativeManifest) {
    final Version manifestVersion = manifestInjector.getCdkVersion(declarativeManifest.getManifest());
    return declarativeManifestImageVersionService
        .getDeclarativeManifestImageVersionByMajorVersion(Integer.parseInt(manifestVersion.getMajorVersion()));
  }

}
