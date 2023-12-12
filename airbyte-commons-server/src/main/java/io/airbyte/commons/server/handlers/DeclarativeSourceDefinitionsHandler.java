/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.DeclarativeManifestVersionRead;
import io.airbyte.api.model.generated.DeclarativeManifestsReadList;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody;
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.DeclarativeSourceManifestInjector;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
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

  private final ConfigRepository configRepository;
  private final DeclarativeSourceManifestInjector manifestInjector;

  @Inject
  public DeclarativeSourceDefinitionsHandler(final ConfigRepository configRepository,
                                             final DeclarativeSourceManifestInjector manifestInjector) {
    this.configRepository = configRepository;
    this.manifestInjector = manifestInjector;
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
      configRepository.createDeclarativeManifestAsActiveVersion(declarativeManifest,
          manifestInjector.createConfigInjection(requestBody.getSourceDefinitionId(), requestBody.getDeclarativeManifest().getManifest()),
          manifestInjector.createDeclarativeManifestConnectorSpecification(spec));
    } else {
      configRepository.insertDeclarativeManifest(declarativeManifest);
    }
    configRepository.deleteManifestDraftForActorDefinition(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
  }

  public void updateDeclarativeManifestVersion(final UpdateActiveManifestRequestBody requestBody) throws IOException, ConfigNotFoundException {
    validateAccessToSource(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
    final Collection<Long> existingVersions = fetchAvailableManifestVersions(requestBody.getSourceDefinitionId());
    if (existingVersions.isEmpty()) {
      throw new SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.getSourceDefinitionId()));
    }

    final DeclarativeManifest declarativeManifest = configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(
        requestBody.getSourceDefinitionId(), requestBody.getVersion());
    configRepository.setDeclarativeSourceActiveVersion(requestBody.getSourceDefinitionId(),
        declarativeManifest.getVersion(),
        manifestInjector.createConfigInjection(declarativeManifest.getActorDefinitionId(), declarativeManifest.getManifest()),
        manifestInjector.createDeclarativeManifestConnectorSpecification(declarativeManifest.getSpec()));
  }

  private Collection<Long> fetchAvailableManifestVersions(final UUID sourceDefinitionId) throws IOException {
    return configRepository.getDeclarativeManifestsByActorDefinitionId(sourceDefinitionId)
        .map(DeclarativeManifest::getVersion)
        .collect(Collectors.toSet());
  }

  private void validateAccessToSource(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    if (!configRepository.workspaceCanUseCustomDefinition(actorDefinitionId, workspaceId)) {
      throw new DeclarativeSourceNotFoundException(
          String.format("Can't find source definition id `%s` in workspace %s", actorDefinitionId, workspaceId));
    }
  }

  public DeclarativeManifestsReadList listManifestVersions(
                                                           final ListDeclarativeManifestsRequestBody requestBody)
      throws IOException, ConfigNotFoundException {
    validateAccessToSource(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
    final Stream<DeclarativeManifest> existingVersions = configRepository.getDeclarativeManifestsByActorDefinitionId(
        requestBody.getSourceDefinitionId());
    final DeclarativeManifest activeVersion =
        configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(requestBody.getSourceDefinitionId());

    return new DeclarativeManifestsReadList().manifestVersions(existingVersions
        .map(manifest -> new DeclarativeManifestVersionRead().description(
            manifest.getDescription()).version(manifest.getVersion()).isActive(manifest.getVersion().equals(activeVersion.getVersion())))
        .sorted(Comparator.comparingLong(DeclarativeManifestVersionRead::getVersion)).collect(Collectors.toList()));
  }

}
