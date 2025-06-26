/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ActorDefinitionVersionHandlerTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition()
      .withSourceDefinitionId(UUID.randomUUID());
  private static final StandardDestinationDefinition DESTINATION_DEFINITION = new StandardDestinationDefinition()
      .withDestinationDefinitionId(UUID.randomUUID());

  private SourceService mSourceService;
  private DestinationService mDestinationService;
  private ActorDefinitionService mActorDefinitionService;
  private ActorDefinitionVersionResolver mActorDefinitionVersionResolver;
  private ActorDefinitionVersionHelper mActorDefinitionVersionHelper;
  private ActorDefinitionHandlerHelper mActorDefinitionHandlerHelper;

  private ActorDefinitionVersionHandler actorDefinitionVersionHandler;
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  @BeforeEach
  void setUp() {
    mSourceService = mock(SourceService.class);
    mDestinationService = mock(DestinationService.class);
    mActorDefinitionService = mock(ActorDefinitionService.class);
    mActorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    mActorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    mActorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    actorDefinitionVersionHandler = new ActorDefinitionVersionHandler(
        mSourceService,
        mDestinationService,
        mActorDefinitionService,
        mActorDefinitionVersionResolver,
        mActorDefinitionVersionHelper,
        mActorDefinitionHandlerHelper,
        apiPojoConverters);
  }

  private ActorDefinitionVersion createActorDefinitionVersion() {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withVersionId(UUID.randomUUID())
        .withSupportLevel(SupportLevel.NONE)
        .withInternalSupportLevel(100L)
        .withReleaseStage(ReleaseStage.BETA)
        .withSupportState(SupportState.SUPPORTED)
        .withDockerRepository("airbyte/source-faker")
        .withDockerImageTag("1.0.2")
        .withDocumentationUrl("https://docs.airbyte.io");
  }

  private ActorDefinitionVersion createActorDefinitionVersionWithNormalization() {
    return createActorDefinitionVersion();
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testGetActorDefinitionVersionForSource(final boolean isVersionOverrideApplied)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID sourceId = UUID.randomUUID();
    final ActorDefinitionVersion actorDefinitionVersion = createActorDefinitionVersion();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(WORKSPACE_ID);

    when(mSourceService.getSourceConnection(sourceId))
        .thenReturn(sourceConnection);
    when(mSourceService.getSourceDefinitionFromSource(sourceId))
        .thenReturn(SOURCE_DEFINITION);
    when(mActorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(SOURCE_DEFINITION, WORKSPACE_ID, sourceId))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied));

    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(sourceId);
    final ActorDefinitionVersionRead actorDefinitionVersionRead =
        actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(sourceIdRequestBody);
    final ActorDefinitionVersionRead expectedRead = new ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(io.airbyte.api.model.generated.SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .supportsFileTransfer(false)
        .supportsDataActivation(false);

    assertEquals(expectedRead, actorDefinitionVersionRead);
    verify(mSourceService).getSourceConnection(sourceId);
    verify(mSourceService).getSourceDefinitionFromSource(sourceId);
    verify(mActorDefinitionVersionHelper).getSourceVersionWithOverrideStatus(SOURCE_DEFINITION, WORKSPACE_ID, sourceId);
    verify(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion);
    verifyNoMoreInteractions(mSourceService);
    verifyNoMoreInteractions(mActorDefinitionHandlerHelper);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
    verifyNoInteractions(mDestinationService);
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testGetActorDefinitionVersionForDestination(final boolean isVersionOverrideApplied)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID destinationId = UUID.randomUUID();
    final ActorDefinitionVersion actorDefinitionVersion = createActorDefinitionVersion();
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID);

    when(mDestinationService.getDestinationConnection(destinationId))
        .thenReturn(destinationConnection);
    when(mDestinationService.getDestinationDefinitionFromDestination(destinationId))
        .thenReturn(DESTINATION_DEFINITION);
    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied));

    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody().destinationId(destinationId);
    final ActorDefinitionVersionRead actorDefinitionVersionRead =
        actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(destinationIdRequestBody);
    final ActorDefinitionVersionRead expectedRead = new ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(io.airbyte.api.model.generated.SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .supportsFileTransfer(false)
        .supportsDataActivation(false);

    assertEquals(expectedRead, actorDefinitionVersionRead);
    verify(mDestinationService).getDestinationConnection(destinationId);
    verify(mDestinationService).getDestinationDefinitionFromDestination(destinationId);
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId);
    verify(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion);
    verifyNoMoreInteractions(mDestinationService);
    verifyNoMoreInteractions(mActorDefinitionHandlerHelper);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testGetActorDefinitionVersionForDestinationWithNormalization(final boolean isVersionOverrideApplied)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID destinationId = UUID.randomUUID();
    final ActorDefinitionVersion actorDefinitionVersion = createActorDefinitionVersionWithNormalization();
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(WORKSPACE_ID);

    when(mDestinationService.getDestinationConnection(destinationId))
        .thenReturn(destinationConnection);
    when(mDestinationService.getDestinationDefinitionFromDestination(destinationId))
        .thenReturn(DESTINATION_DEFINITION);
    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isVersionOverrideApplied));

    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody().destinationId(destinationId);
    final ActorDefinitionVersionRead actorDefinitionVersionRead =
        actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(destinationIdRequestBody);
    final ActorDefinitionVersionRead expectedRead = new ActorDefinitionVersionRead()
        .isVersionOverrideApplied(isVersionOverrideApplied)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(io.airbyte.api.model.generated.SupportState.SUPPORTED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(actorDefinitionVersion.getSupportsRefreshes())
        .supportsFileTransfer(false)
        .supportsDataActivation(false);

    assertEquals(expectedRead, actorDefinitionVersionRead);
    verify(mDestinationService).getDestinationConnection(destinationId);
    verify(mDestinationService).getDestinationDefinitionFromDestination(destinationId);
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(DESTINATION_DEFINITION, WORKSPACE_ID, destinationId);
    verify(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion);
    verifyNoMoreInteractions(mDestinationService);
    verifyNoMoreInteractions(mActorDefinitionHandlerHelper);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
    verifyNoInteractions(mSourceService);
  }

  @Test
  void testCreateActorDefinitionVersionReadWithBreakingChange() throws IOException {
    final ActorDefinitionVersionBreakingChanges breakingChanges = mock(ActorDefinitionVersionBreakingChanges.class);

    final ActorDefinitionVersion actorDefinitionVersion = createActorDefinitionVersion().withSupportState(SupportState.DEPRECATED);
    when(mActorDefinitionHandlerHelper.getVersionBreakingChanges(actorDefinitionVersion)).thenReturn(Optional.of(breakingChanges));

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        new ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, false);
    final ActorDefinitionVersionRead actorDefinitionVersionRead =
        actorDefinitionVersionHandler.createActorDefinitionVersionRead(versionWithOverrideStatus);

    final ActorDefinitionVersionRead expectedRead = new ActorDefinitionVersionRead()
        .isVersionOverrideApplied(false)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.NONE)
        .supportState(io.airbyte.api.model.generated.SupportState.DEPRECATED)
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportsRefreshes(false)
        .breakingChanges(breakingChanges)
        .supportsFileTransfer(false)
        .supportsDataActivation(false);

    assertEquals(expectedRead, actorDefinitionVersionRead);
    verify(mActorDefinitionHandlerHelper).getVersionBreakingChanges(actorDefinitionVersion);
    verifyNoMoreInteractions(mActorDefinitionHandlerHelper);
    verifyNoInteractions(mActorDefinitionVersionHelper);
    verifyNoInteractions(mSourceService);
    verifyNoInteractions(mDestinationService);
  }

  @Test
  void testResolveActorDefinitionVersionByTag() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID actorDefinitionId = UUID.randomUUID();
    final ActorDefinitionVersion actorDefinitionVersion = createActorDefinitionVersion();
    final ResolveActorDefinitionVersionResponse resolveActorDefinitionVersionResponse = new ResolveActorDefinitionVersionResponse()
        .versionId(actorDefinitionVersion.getVersionId())
        .dockerRepository(actorDefinitionVersion.getDockerRepository())
        .dockerImageTag(actorDefinitionVersion.getDockerImageTag())
        .supportRefreshes(false)
        .supportFileTransfer(false)
        .supportDataActivation(false);

    when(mSourceService.getStandardSourceDefinition(actorDefinitionId))
        .thenReturn(Jsons.clone(SOURCE_DEFINITION).withDefaultVersionId(actorDefinitionVersion.getVersionId()));
    when(mActorDefinitionService.getActorDefinitionVersion(actorDefinitionVersion.getVersionId()))
        .thenReturn(actorDefinitionVersion);
    when(mActorDefinitionVersionResolver.resolveVersionForTag(actorDefinitionId, ActorType.SOURCE,
        actorDefinitionVersion.getDockerRepository(), actorDefinitionVersion.getDockerImageTag()))
            .thenReturn(Optional.of(actorDefinitionVersion));

    final ResolveActorDefinitionVersionResponse resolvedActorDefinitionVersion =
        actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(new ResolveActorDefinitionVersionRequestBody()
            .actorDefinitionId(actorDefinitionId)
            .actorType(io.airbyte.api.model.generated.ActorType.SOURCE)
            .dockerImageTag(actorDefinitionVersion.getDockerImageTag()));

    assertEquals(resolveActorDefinitionVersionResponse, resolvedActorDefinitionVersion);
    verify(mActorDefinitionVersionResolver).resolveVersionForTag(actorDefinitionId, ActorType.SOURCE,
        actorDefinitionVersion.getDockerRepository(), actorDefinitionVersion.getDockerImageTag());
    verify(mActorDefinitionService).getActorDefinitionVersion(actorDefinitionVersion.getVersionId());
    verifyNoMoreInteractions(mActorDefinitionVersionResolver);
    verifyNoMoreInteractions(mActorDefinitionService);
    verifyNoInteractions(mActorDefinitionVersionHelper);
    verifyNoInteractions(mDestinationService);
  }

  @Test
  void testResolveMissingActorDefinitionVersionByTag() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID actorDefinitionId = UUID.randomUUID();
    final UUID defaultVersionId = UUID.randomUUID();
    final String dockerRepository = "airbyte/source-pg";
    final String dockerImageTag = "1.0.2";

    when(mSourceService.getStandardSourceDefinition(actorDefinitionId))
        .thenReturn(Jsons.clone(SOURCE_DEFINITION).withDefaultVersionId(defaultVersionId));
    when(mActorDefinitionService.getActorDefinitionVersion(defaultVersionId))
        .thenReturn(new ActorDefinitionVersion().withDockerRepository(dockerRepository));

    when(mActorDefinitionVersionResolver.resolveVersionForTag(actorDefinitionId, ActorType.SOURCE, dockerRepository, dockerImageTag))
        .thenReturn(Optional.empty());

    final ResolveActorDefinitionVersionRequestBody resolveVersionRequestBody = new ResolveActorDefinitionVersionRequestBody()
        .actorDefinitionId(actorDefinitionId)
        .actorType(io.airbyte.api.model.generated.ActorType.SOURCE)
        .dockerImageTag(dockerImageTag);

    final NotFoundException exception =
        assertThrows(NotFoundException.class, () -> actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(resolveVersionRequestBody));
    assertEquals(String.format("Could not find actor definition version for actor definition id %s and tag %s",
        actorDefinitionId, dockerImageTag), exception.getMessage());
    verify(mActorDefinitionVersionResolver).resolveVersionForTag(actorDefinitionId, ActorType.SOURCE, dockerRepository, dockerImageTag);
    verifyNoMoreInteractions(mActorDefinitionVersionResolver);
    verify(mActorDefinitionService).getActorDefinitionVersion(defaultVersionId);
    verifyNoMoreInteractions(mActorDefinitionService);
    verifyNoInteractions(mActorDefinitionVersionHelper);
    verifyNoInteractions(mDestinationService);
  }

}
