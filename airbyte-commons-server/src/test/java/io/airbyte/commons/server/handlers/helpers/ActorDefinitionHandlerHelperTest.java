/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleases;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.featureflag.IngestBreakingChanges;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.micronaut.http.uri.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ActorDefinitionHandlerHelperTest {

  private SynchronousSchedulerClient synchronousSchedulerClient;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private ActorDefinitionVersionResolver actorDefinitionVersionResolver;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private ConfigRepository configRepository;
  private TestClient featureFlagClient;

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String VALID_PROTOCOL_VERSION = "0.1.0";
  private static final String INVALID_PROTOCOL_VERSION = "123.0.0";
  private static final URI DOCUMENTATION_URL = UriBuilder.of("").scheme("https").host("docs.com").build();

  private static final String LATEST = "latest";

  private static final ConnectorRegistrySourceDefinition connectorRegistrySourceDefinition =
      new ConnectorRegistrySourceDefinition()
          .withSourceDefinitionId(ACTOR_DEFINITION_ID)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
          .withProtocolVersion(VALID_PROTOCOL_VERSION);

  private static final ActorDefinitionVersion actorDefinitionVersion =
      new ActorDefinitionVersion()
          .withActorDefinitionId(ACTOR_DEFINITION_ID)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
          .withProtocolVersion(VALID_PROTOCOL_VERSION);

  @BeforeEach
  void setUp() {
    synchronousSchedulerClient = spy(SynchronousSchedulerClient.class);
    final AirbyteProtocolVersionRange protocolVersionRange = new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.3.0"));
    actorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    configRepository = mock(ConfigRepository.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    featureFlagClient = mock(TestClient.class);
    actorDefinitionHandlerHelper =
        new ActorDefinitionHandlerHelper(synchronousSchedulerClient, protocolVersionRange, actorDefinitionVersionResolver, configRepository,
            remoteDefinitionsProvider, featureFlagClient);

    when(featureFlagClient.boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);
  }

  @Nested
  class TestDefaultDefinitionVersionFromCreate {

    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should always be custom")
    void testDefaultDefinitionVersionFromCreate() throws IOException {
      when(synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID))
          .thenReturn(new SynchronousResponse<>(
              new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion expectedNewVersion = new ActorDefinitionVersion()
          .withActorDefinitionId(null)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withDockerRepository(DOCKER_REPOSITORY)
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION))
          .withDocumentationUrl(DOCUMENTATION_URL.toString())
          .withProtocolVersion(VALID_PROTOCOL_VERSION)
          .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, DOCUMENTATION_URL, WORKSPACE_ID);
      verify(synchronousSchedulerClient).createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID);
      assertEquals(expectedNewVersion, newVersion);

      verifyNoMoreInteractions(synchronousSchedulerClient);
      verifyNoInteractions(actorDefinitionVersionResolver, configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

    @Test
    @DisplayName("Creating an ActorDefinitionVersion from create with an invalid protocol version should throw an exception")
    void testDefaultDefinitionVersionFromCreateInvalidProtocolVersionThrows() throws IOException {
      when(synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID))
          .thenReturn(new SynchronousResponse<>(
              new ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      assertThrows(UnsupportedProtocolVersionException.class,
          () -> actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, DOCUMENTATION_URL,
              WORKSPACE_ID));
      verify(synchronousSchedulerClient).createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID);

      verifyNoMoreInteractions(synchronousSchedulerClient);
      verifyNoInteractions(actorDefinitionVersionResolver, configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

  }

  @Nested
  class TestDefaultDefinitionVersionFromUpdate {

    @BeforeEach
    void setUp() throws IOException {
      // default version resolver to not have the new version already
      when(actorDefinitionVersionResolver.resolveVersionForTag(eq(actorDefinitionVersion.getActorDefinitionId()), eq(ActorType.SOURCE),
          eq(actorDefinitionVersion.getDockerRepository()), any())).thenReturn(Optional.empty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update with a new version gets a new spec and new protocol version")
    void testDefaultDefinitionVersionFromUpdateNewVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;
      final String newDockerImageTag = "newTag";
      final String newValidProtocolVersion = "0.2.0";
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      final ConnectorSpecification newSpec =
          new ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty("Something", "new");

      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, null)).thenReturn(new SynchronousResponse<>(
          newSpec, SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);

      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);

      assertNotEquals(previousDefaultVersion, newVersion);
      assertEquals(newSpec, newVersion.getSpec());
      assertEquals(newValidProtocolVersion, newVersion.getProtocolVersion());

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("If the 'new' version has the same dockerImageTag, we don't attempt to fetch a new spec")
    void testDefaultDefinitionVersionFromUpdateSameVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;
      final String newDockerImageTag = previousDefaultVersion.getDockerImageTag();
      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(previousDefaultVersion, newVersion);

      verifyNoMoreInteractions(actorDefinitionVersionResolver);
      verifyNoInteractions(synchronousSchedulerClient, configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Always fetch specs for dev versions")
    void testDefaultDefinitionVersionFromUpdateSameDevVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = Jsons.clone(actorDefinitionVersion).withDockerImageTag("dev");
      final String newDockerImageTag = "dev";
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      final String newValidProtocolVersion = "0.2.0";

      final ConnectorSpecification newSpec =
          new ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty("Something", "new");
      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, null)).thenReturn(new SynchronousResponse<>(
          newSpec, SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(previousDefaultVersion.getDockerImageTag(), newVersion.getDockerImageTag());
      assertNotEquals(previousDefaultVersion, newVersion);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update with an invalid protocol version should throw an exception")
    void testDefaultDefinitionVersionFromUpdateInvalidProtocolVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;
      final String newDockerImageTag = "newTag";
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, null)).thenReturn(new SynchronousResponse<>(
          new ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      assertThrows(UnsupportedProtocolVersionException.class,
          () -> actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector));
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update should return an already existing one from db/remote before creating a new one")
    void testDefaultDefinitionVersioFromUpdateVersionResolved(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;

      final String newDockerImageTag = "newTagButPreviouslyUsed";
      final ActorDefinitionVersion oldExistingADV = Jsons.clone(actorDefinitionVersion).withDockerImageTag(newDockerImageTag)
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty("Something", "existing"));

      when(actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, newDockerImageTag))
          .thenReturn(Optional.of(oldExistingADV));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(oldExistingADV, newVersion);

      verifyNoMoreInteractions(actorDefinitionVersionResolver);
      verifyNoInteractions(synchronousSchedulerClient, configRepository, remoteDefinitionsProvider, featureFlagClient);
    }

  }

  @Nested
  class TestPersistBreakingChange {

    @Test
    void testPersistBreakingChanges() throws IOException {
      final BreakingChanges registryBreakingChanges =
          new BreakingChanges().withAdditionalProperty("1.0.0", new VersionBreakingChange().withMessage("A breaking change was made")
              .withUpgradeDeadline("2000-01-01").withMigrationDocumentationUrl("https://docs.airbyte.io/migration"));
      final ConnectorRegistrySourceDefinition sourceDefWithBreakingChanges =
          Jsons.clone(connectorRegistrySourceDefinition).withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

      when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST))
          .thenReturn(Optional.of(sourceDefWithBreakingChanges));

      actorDefinitionHandlerHelper.persistBreakingChanges(actorDefinitionVersion, ActorType.SOURCE);
      verify(featureFlagClient).boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS));
      verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);
      verify(configRepository)
          .writeActorDefinitionBreakingChanges(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(sourceDefWithBreakingChanges));

      verifyNoMoreInteractions(configRepository, featureFlagClient, remoteDefinitionsProvider);
      verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
    }

    @Test
    void testPersistNoBreakingChanges() throws IOException {
      when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST))
          .thenReturn(Optional.of(connectorRegistrySourceDefinition));

      actorDefinitionHandlerHelper.persistBreakingChanges(actorDefinitionVersion, ActorType.SOURCE);
      verify(featureFlagClient).boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS));
      verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);
      verify(configRepository).writeActorDefinitionBreakingChanges(List.of());

      verifyNoMoreInteractions(configRepository, featureFlagClient, remoteDefinitionsProvider);
      verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
    }

    @Test
    void testBreakingChangesAreNotPersistedIfDefinitionNotFound() throws IOException {
      when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)).thenReturn(Optional.empty());

      actorDefinitionHandlerHelper.persistBreakingChanges(actorDefinitionVersion, ActorType.SOURCE);
      verify(featureFlagClient).boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS));
      verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);

      verifyNoMoreInteractions(configRepository, featureFlagClient, remoteDefinitionsProvider);
      verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
    }

    @Test
    void testTurnOffBreakingChangesFeatureFlag() throws IOException {
      when(featureFlagClient.boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(false);

      actorDefinitionHandlerHelper.persistBreakingChanges(actorDefinitionVersion, ActorType.SOURCE);
      verify(featureFlagClient).boolVariation(IngestBreakingChanges.INSTANCE, new Workspace(ANONYMOUS));

      verifyNoMoreInteractions(featureFlagClient);
      verifyNoInteractions(configRepository, synchronousSchedulerClient, actorDefinitionVersionResolver, remoteDefinitionsProvider);
    }

  }

  private String getDockerImageForTag(final String dockerImageTag) {
    return String.format("%s:%s", DOCKER_REPOSITORY, dockerImageTag);
  }

}
