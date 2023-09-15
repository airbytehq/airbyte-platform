/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.AdvancedAuth;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.converters.OauthModelConverter;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * This class is responsible for getting the specification for a given connector. It is used by the
 * {@link io.airbyte.commons.server.handlers.SchedulerHandler} to get the specification for a
 * connector.
 */
@Singleton
public class ConnectorDefinitionSpecificationHandler {

  private final ConfigRepository configRepository;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final JobConverter jobConverter;

  public ConnectorDefinitionSpecificationHandler(final ConfigRepository configRepository,
                                                 final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                                 final JobConverter jobConverter) {
    this.configRepository = configRepository;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.jobConverter = jobConverter;
  }

  /**
   * Get the specification for a given source.
   *
   * @param sourceIdRequestBody - the id of the source to get the specification for.
   * @return the specification for the source.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the source does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  public SourceDefinitionSpecificationRead getSpecificationForSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceConnection source = configRepository.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), sourceIdRequestBody.getSourceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(sourceDefinition, spec);
  }

  /**
   * Get the definition specification for a given source.
   *
   * @param sourceDefinitionIdWithWorkspaceId - the id of the source to get the specification for.
   * @return the specification for the source.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the source does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  public SourceDefinitionSpecificationRead getSourceDefinitionSpecification(final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID sourceDefinitionId = sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId();
    final StandardSourceDefinition source = configRepository.getStandardSourceDefinition(sourceDefinitionId);
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(source, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(source, spec);
  }

  /**
   * Get the specification for a given destination.
   *
   * @param destinationIdRequestBody - the id of the destination to get the specification for.
   * @return the specification for the destination.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the destination does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  public DestinationDefinitionSpecificationRead getSpecificationForDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final DestinationConnection destination = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(),
            destinationIdRequestBody.getDestinationId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = destinationVersion.getSpec();
    return getDestinationSpecificationRead(destinationDefinition, spec);
  }

  /**
   * Get the definition specification for a given destination.
   *
   * @param destinationDefinitionIdWithWorkspaceId - the id of the destination to get the
   *        specification for.
   * @return the specification for the destination.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the destination does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  @SuppressWarnings("LineLength")
  public DestinationDefinitionSpecificationRead getDestinationSpecification(final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID destinationDefinitionId = destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId();
    final StandardDestinationDefinition destination = configRepository.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = destinationVersion.getSpec();

    return getDestinationSpecificationRead(destination, spec);
  }

  private SourceDefinitionSpecificationRead getSourceSpecificationRead(final StandardSourceDefinition sourceDefinition,
                                                                       final io.airbyte.protocol.models.ConnectorSpecification spec) {
    final SourceDefinitionSpecificationRead specRead = new SourceDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(JobConfig.ConfigType.GET_SPEC)))
        .connectionSpecification(spec.getConnectionSpecification())
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId());

    if (spec.getDocumentationUrl() != null) {
      specRead.documentationUrl(spec.getDocumentationUrl().toString());
    }

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);

    return specRead;
  }

  private DestinationDefinitionSpecificationRead getDestinationSpecificationRead(final StandardDestinationDefinition destinationDefinition,
                                                                                 final io.airbyte.protocol.models.ConnectorSpecification spec) {
    final DestinationDefinitionSpecificationRead specRead = new DestinationDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(JobConfig.ConfigType.GET_SPEC)))
        .supportedDestinationSyncModes(Enums.convertListTo(spec.getSupportedDestinationSyncModes(), DestinationSyncMode.class))
        .connectionSpecification(spec.getConnectionSpecification())
        .documentationUrl(spec.getDocumentationUrl().toString())
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId());

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);

    return specRead;
  }

}
