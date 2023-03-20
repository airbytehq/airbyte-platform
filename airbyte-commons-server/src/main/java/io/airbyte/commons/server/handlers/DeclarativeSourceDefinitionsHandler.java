/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException;
import io.airbyte.commons.server.errors.SourceIsNotDeclarativeException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.ConnectorBuilderSpecAdapter;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

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

  public void createDeclarativeSourceDefinitionManifest(DeclarativeSourceDefinitionCreateManifestRequestBody requestBody) throws IOException {
    if (!configRepository.workspaceCanUseCustomDefinition(requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId())) {
      throw new DeclarativeSourceNotFoundException(
          String.format("Can't find source definition id `%s` in workspace %s", requestBody.getSourceDefinitionId(), requestBody.getWorkspaceId()));
    }
    Set<Long> existingVersions = configRepository.getDeclarativeManifestsByActorDefinitionId(
        requestBody.getSourceDefinitionId()).map(DeclarativeManifest::getVersion).collect(Collectors.toSet());

    long version = requestBody.getDeclarativeManifest().getVersion().longValue();
    if (existingVersions.isEmpty()) {
      throw new SourceIsNotDeclarativeException(
          String.format("Source %s is does not have a declarative manifest associated to it", requestBody.getSourceDefinitionId()));
    } else if (existingVersions.contains(version)) {
      throw new ValueConflictKnownException(String.format("Version '%s' for source %s already exists", version, requestBody.getSourceDefinitionId()));
    }

    DeclarativeManifest declarativeManifest = new DeclarativeManifest()
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

}
