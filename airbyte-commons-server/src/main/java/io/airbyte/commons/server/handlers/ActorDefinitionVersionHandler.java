/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.converters.ApiPojoConverters.toApiSupportLevel;
import static io.airbyte.commons.server.converters.ApiPojoConverters.toApiSupportState;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * ActorDefinitionVersionHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings("ParameterName")
@Singleton
public class ActorDefinitionVersionHandler {

  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final ActorDefinitionService actorDefinitionService;
  private final ActorDefinitionVersionResolver actorDefinitionVersionResolver;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;

  @Inject
  public ActorDefinitionVersionHandler(final SourceService sourceService,
                                       final DestinationService destinationService,
                                       final ActorDefinitionService actorDefinitionService,
                                       final ActorDefinitionVersionResolver actorDefinitionVersionResolver,
                                       final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                       final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper) {
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.actorDefinitionService = actorDefinitionService;
    this.actorDefinitionVersionResolver = actorDefinitionVersionResolver;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
  }

  public ActorDefinitionVersionRead getActorDefinitionVersionForSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException, ConfigNotFoundException {
    final SourceConnection sourceConnection = sourceService.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId());
    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, sourceConnection.getWorkspaceId(),
            sourceConnection.getSourceId());
    return createActorDefinitionVersionRead(versionWithOverrideStatus);
  }

  private ActorDefinitionVersion getDefaultVersion(final ActorType actorType, final UUID actorDefinitionId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return switch (actorType) {
      case SOURCE -> actorDefinitionService
          .getActorDefinitionVersion(sourceService.getStandardSourceDefinition(actorDefinitionId).getDefaultVersionId());
      case DESTINATION -> actorDefinitionService.getActorDefinitionVersion(
          destinationService.getStandardDestinationDefinition(actorDefinitionId).getDefaultVersionId());
    };
  }

  public ResolveActorDefinitionVersionResponse resolveActorDefinitionVersionByTag(final ResolveActorDefinitionVersionRequestBody resolveVersionReq)
      throws JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException, IOException {
    final UUID actorDefinitionId = resolveVersionReq.getActorDefinitionId();
    final ActorType actorType = ApiPojoConverters.toInternalActorType(resolveVersionReq.getActorType());
    final ActorDefinitionVersion defaultVersion = getDefaultVersion(actorType, actorDefinitionId);

    final Optional<ActorDefinitionVersion> optResolvedVersion = actorDefinitionVersionResolver.resolveVersionForTag(
        actorDefinitionId,
        actorType,
        defaultVersion.getDockerRepository(),
        resolveVersionReq.getDockerImageTag());

    if (optResolvedVersion.isEmpty()) {
      throw new NotFoundException(String.format("Could not find actor definition version for actor definition id %s and tag %s",
          actorDefinitionId, resolveVersionReq.getDockerImageTag()));
    }

    final ActorDefinitionVersion resolvedVersion = optResolvedVersion.get();

    return new ResolveActorDefinitionVersionResponse().versionId(resolvedVersion.getVersionId()).dockerImageTag(resolvedVersion.getDockerImageTag())
        .dockerRepository(resolvedVersion.getDockerRepository());
  }

  public ActorDefinitionVersionRead getActorDefinitionVersionForDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationConnection destinationConnection = destinationService.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId());
    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition,
            destinationConnection.getWorkspaceId(), destinationConnection.getDestinationId());
    return createActorDefinitionVersionRead(versionWithOverrideStatus);
  }

  @VisibleForTesting
  ActorDefinitionVersionRead createActorDefinitionVersionRead(final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus)
      throws IOException {
    final ActorDefinitionVersion actorDefinitionVersion = versionWithOverrideStatus.actorDefinitionVersion();
    final ActorDefinitionVersionRead advRead = new ActorDefinitionVersionRead()
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsDbt(Objects.requireNonNullElse(actorDefinitionVersion.getSupportsDbt(), false))
        .normalizationConfig(ApiPojoConverters.normalizationDestinationDefinitionConfigToApi(actorDefinitionVersion.getNormalizationConfig()))
        .supportState(toApiSupportState(actorDefinitionVersion.getSupportState()))
        .supportLevel(toApiSupportLevel(actorDefinitionVersion.getSupportLevel()))
        .isVersionOverrideApplied(versionWithOverrideStatus.isOverrideApplied());

    final Optional<ActorDefinitionVersionBreakingChanges> breakingChanges =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(actorDefinitionVersion);
    breakingChanges.ifPresent(advRead::setBreakingChanges);

    return advRead;
  }

}
