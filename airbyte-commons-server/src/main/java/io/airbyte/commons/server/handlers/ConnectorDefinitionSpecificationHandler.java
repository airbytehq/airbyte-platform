/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AdvancedAuth;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.converters.OauthModelConverter;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.JobConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.SourceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * This class is responsible for getting the specification for a given connector. It is used by the
 * {@link io.airbyte.commons.server.handlers.SchedulerHandler} to get the specification for a
 * connector.
 */
@Singleton
public class ConnectorDefinitionSpecificationHandler {

  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final JobConverter jobConverter;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final OAuthService oAuthService;

  public ConnectorDefinitionSpecificationHandler(final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                                 final JobConverter jobConverter,
                                                 final SourceService sourceService,
                                                 final DestinationService destinationService,
                                                 final OAuthService oauthService) {
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.jobConverter = jobConverter;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.oAuthService = oauthService;
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
    final SourceConnection source = sourceService.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), sourceIdRequestBody.getSourceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(sourceDefinition, spec, source.getWorkspaceId());
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
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID sourceDefinitionId = sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId();
    final StandardSourceDefinition source = sourceService.getStandardSourceDefinition(sourceDefinitionId);
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(source, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(source, spec, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
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
    final DestinationConnection destination = destinationService.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(),
            destinationIdRequestBody.getDestinationId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = destinationVersion.getSpec();
    return getDestinationSpecificationRead(destinationDefinition, spec, destinationVersion.getSupportsRefreshes(), destination.getWorkspaceId());
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
    final StandardDestinationDefinition destination = destinationService.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    final io.airbyte.protocol.models.ConnectorSpecification spec = destinationVersion.getSpec();

    return getDestinationSpecificationRead(destination, spec, destinationVersion.getSupportsRefreshes(),
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
  }

  @VisibleForTesting
  SourceDefinitionSpecificationRead getSourceSpecificationRead(final StandardSourceDefinition sourceDefinition,
                                                               final io.airbyte.protocol.models.ConnectorSpecification spec,
                                                               final UUID workspaceId)
      throws IOException {
    final SourceDefinitionSpecificationRead specRead = new SourceDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(JobConfig.ConfigType.GET_SPEC)))
        .connectionSpecification(spec.getConnectionSpecification())
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId());

    if (spec.getDocumentationUrl() != null) {
      specRead.documentationUrl(spec.getDocumentationUrl().toString());
    }

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);
    if (advancedAuth.isPresent()) {
      final Optional<SourceOAuthParameter> sourceOAuthParameter =
          oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinition.getSourceDefinitionId());
      specRead.setAdvancedAuthGlobalCredentialsAvailable(sourceOAuthParameter.isPresent());
    }

    return specRead;
  }

  @VisibleForTesting
  DestinationDefinitionSpecificationRead getDestinationSpecificationRead(final StandardDestinationDefinition destinationDefinition,
                                                                         final io.airbyte.protocol.models.ConnectorSpecification spec,
                                                                         final boolean supportsRefreshes,
                                                                         final UUID workspaceId)
      throws IOException {
    final DestinationDefinitionSpecificationRead specRead = new DestinationDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(JobConfig.ConfigType.GET_SPEC)))
        .supportedDestinationSyncModes(getFinalDestinationSyncModes(spec.getSupportedDestinationSyncModes(), supportsRefreshes))
        .connectionSpecification(spec.getConnectionSpecification())
        .documentationUrl(spec.getDocumentationUrl().toString())
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId());

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);
    if (advancedAuth.isPresent()) {
      final Optional<DestinationOAuthParameter> destinationOAuthParameter =
          oAuthService.getDestinationOAuthParameterOptional(workspaceId, destinationDefinition.getDestinationDefinitionId());
      specRead.setAdvancedAuthGlobalCredentialsAvailable(destinationOAuthParameter.isPresent());
    }

    return specRead;
  }

  private List<DestinationSyncMode> getFinalDestinationSyncModes(final List<io.airbyte.protocol.models.DestinationSyncMode> syncModes,
                                                                 final boolean supportsRefreshes) {
    final List<DestinationSyncMode> finalSyncModes = new ArrayList<>();
    boolean hasDedup = false;
    boolean hasOverwrite = false;
    for (final var syncMode : syncModes) {
      switch (syncMode) {
        case APPEND -> finalSyncModes.add(DestinationSyncMode.APPEND);
        case APPEND_DEDUP -> {
          finalSyncModes.add(DestinationSyncMode.APPEND_DEDUP);
          hasDedup = true;
        }
        case OVERWRITE -> {
          finalSyncModes.add(DestinationSyncMode.OVERWRITE);
          hasOverwrite = true;
        }
        default -> throw new IllegalStateException("Unexpected value: " + syncMode);
      }
    }
    if (supportsRefreshes && hasDedup && hasOverwrite) {
      finalSyncModes.add(DestinationSyncMode.OVERWRITE_DEDUP);
    }
    return finalSyncModes;
  }

}
