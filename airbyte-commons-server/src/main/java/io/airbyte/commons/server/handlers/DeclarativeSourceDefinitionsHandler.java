/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.DeclarativeManifestVersionRead;
import io.airbyte.api.model.generated.DeclarativeManifestsReadList;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DeclarativeSourceDefinitionsHandler. Javadocs suppressed because api docs should be used as
 * source of truth.
 */
@SuppressWarnings({"MissingJavadocMethod"})
@Singleton
public class DeclarativeSourceDefinitionsHandler {

  private final ConfigRepository configRepository;
  private final ConnectorBuilderSpecAdapter specAdapter;

  @Inject
  public DeclarativeSourceDefinitionsHandler(final ConfigRepository configRepository,
                                             final ConnectorBuilderSpecAdapter specAdapter) {
    this.configRepository = configRepository;
    this.specAdapter = specAdapter;
  }

  private void validateDeclarativeSourceDefinition(final UUID sourceDefinitionId, final UUID workspaceId) throws IOException {
    if (!configRepository.workspaceCanUseCustomDefinition(sourceDefinitionId, workspaceId)) {
      throw new DeclarativeSourceNotFoundException(
          String.format("Can't find source definition id `%s` in workspace %s", sourceDefinitionId, workspaceId));
    }
  }

  public void createDeclarativeSourceDefinitionManifest(final DeclarativeSourceDefinitionCreateManifestRequestBody requestBody) throws IOException {
    validateDeclarativeSourceDefinition(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
    final Set<Long> existingVersions = configRepository.getDeclarativeManifestsByActorDefinitionId(
        requestBody.getSourceDefinitionId()).map(DeclarativeManifest::getVersion).collect(Collectors.toSet());

    final long version = requestBody.getDeclarativeManifest().getVersion().longValue();
    if (existingVersions.isEmpty()) {
      throw new SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.getSourceDefinitionId()));
    } else if (existingVersions.contains(version)) {
      throw new ValueConflictKnownException(String.format("Version '%s' for source %s already exists", version, requestBody.getSourceDefinitionId()));
    }

    final DeclarativeManifest declarativeManifest = new DeclarativeManifest()
        .withActorDefinitionId(requestBody.getSourceDefinitionId())
        .withVersion(version)
        .withDescription(requestBody.getDeclarativeManifest().getDescription())
        .withManifest(requestBody.getDeclarativeManifest().getManifest())
        .withSpec(requestBody.getDeclarativeManifest().getSpec());
    if (requestBody.getSetAsActiveManifest()) {
      final ConnectorSpecification connectorSpecification = specAdapter.adapt(requestBody.getDeclarativeManifest().getSpec());
      configRepository.updateDeclarativeActorDefinition(requestBody.getSourceDefinitionId(), requestBody.getDeclarativeManifest().getManifest(),
          connectorSpecification);
      configRepository.insertActiveDeclarativeManifest(declarativeManifest);
    } else {
      configRepository.insertDeclarativeManifest(declarativeManifest);
    }
  }

  public DeclarativeManifestsReadList listManifestVersions(
                                                           final ListDeclarativeManifestsRequestBody requestBody)
      throws IOException, ConfigNotFoundException {
    validateDeclarativeSourceDefinition(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId());
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
