/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

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

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.DeadlineAction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleasesSource;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.micronaut.http.uri.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.Collections;
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
  private ActorDefinitionService actorDefinitionService;
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String VALID_PROTOCOL_VERSION = "0.1.0";
  private static final String INVALID_PROTOCOL_VERSION = "123.0.0";
  private static final URI DOCUMENTATION_URL = UriBuilder.of("").scheme("https").host("docs.com").build();
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  private static final String LATEST = "latest";
  private static final String DEV = "dev";
  private static final String SPEC_KEY = "Something";

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
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    actorDefinitionService = mock(ActorDefinitionService.class);
    actorDefinitionHandlerHelper = new ActorDefinitionHandlerHelper(
        synchronousSchedulerClient, protocolVersionRange, actorDefinitionVersionResolver, remoteDefinitionsProvider, actorDefinitionService,
        apiPojoConverters);

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
          .withSupportLevel(SupportLevel.NONE)
          .withInternalSupportLevel(100L)
          .withProtocolVersion(VALID_PROTOCOL_VERSION)
          .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, DOCUMENTATION_URL, WORKSPACE_ID);
      verify(synchronousSchedulerClient).createGetSpecJob(getDockerImageForTag(DOCKER_IMAGE_TAG), true, WORKSPACE_ID);
      assertEquals(expectedNewVersion, newVersion);

      verifyNoMoreInteractions(synchronousSchedulerClient);
      verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider);
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
      verifyNoInteractions(actorDefinitionVersionResolver, remoteDefinitionsProvider);
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
          new ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new");

      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)).thenReturn(new SynchronousResponse<>(
          newSpec, SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector, WORKSPACE_ID);

      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID);

      assertNotEquals(previousDefaultVersion, newVersion);
      assertEquals(newSpec, newVersion.getSpec());
      assertEquals(newValidProtocolVersion, newVersion.getProtocolVersion());

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(remoteDefinitionsProvider);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("If the 'new' version has the same dockerImageTag, we don't attempt to fetch a new spec")
    void testDefaultDefinitionVersionFromUpdateSameVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;
      final String newDockerImageTag = previousDefaultVersion.getDockerImageTag();

      when(actorDefinitionVersionResolver.resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(), ActorType.SOURCE,
          previousDefaultVersion.getDockerRepository(), newDockerImageTag))
              .thenReturn(Optional.of(previousDefaultVersion));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector, WORKSPACE_ID);

      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(previousDefaultVersion, newVersion);

      verifyNoMoreInteractions(actorDefinitionVersionResolver);
      verifyNoInteractions(synchronousSchedulerClient, remoteDefinitionsProvider);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Always fetch specs for dev versions")
    void testDefaultDefinitionVersionFromUpdateSameDevVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = Jsons.clone(actorDefinitionVersion).withDockerImageTag(DEV);
      final String newDockerImageTag = DEV;
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      final String newValidProtocolVersion = "0.2.0";

      final ConnectorSpecification newSpec =
          new ConnectorSpecification().withProtocolVersion(newValidProtocolVersion).withAdditionalProperty(SPEC_KEY, "new");
      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)).thenReturn(new SynchronousResponse<>(
          newSpec, SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector, WORKSPACE_ID);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(previousDefaultVersion.getDockerImageTag(), newVersion.getDockerImageTag());
      assertNotEquals(previousDefaultVersion, newVersion);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID);

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(remoteDefinitionsProvider);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update with an invalid protocol version should throw an exception")
    void testDefaultDefinitionVersionFromUpdateInvalidProtocolVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;
      final String newDockerImageTag = "newTag";
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID)).thenReturn(new SynchronousResponse<>(
          new ConnectorSpecification().withProtocolVersion(INVALID_PROTOCOL_VERSION), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      assertThrows(UnsupportedProtocolVersionException.class,
          () -> actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector, WORKSPACE_ID));
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, WORKSPACE_ID);

      verifyNoMoreInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
      verifyNoInteractions(remoteDefinitionsProvider);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update should return an already existing one from db/remote before creating a new one")
    void testDefaultDefinitionVersionFromUpdateVersionResolved(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = actorDefinitionVersion;

      final String newDockerImageTag = "newTagButPreviouslyUsed";
      final ActorDefinitionVersion oldExistingADV = Jsons.clone(actorDefinitionVersion).withDockerImageTag(newDockerImageTag)
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"));

      when(actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, newDockerImageTag))
          .thenReturn(Optional.of(oldExistingADV));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector, WORKSPACE_ID);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), newDockerImageTag);

      assertEquals(oldExistingADV, newVersion);

      verifyNoMoreInteractions(actorDefinitionVersionResolver);
      verifyNoInteractions(synchronousSchedulerClient, remoteDefinitionsProvider);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Re-fetch spec for dev versions resolved from the db")
    void testUpdateVersionResolvedDevVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = Jsons.clone(actorDefinitionVersion).withDockerImageTag(DEV);
      final String dockerImage = getDockerImageForTag(DEV);

      final ConnectorSpecification newSpec =
          new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "new");
      when(synchronousSchedulerClient.createGetSpecJob(dockerImage, isCustomConnector, WORKSPACE_ID)).thenReturn(new SynchronousResponse<>(
          newSpec, SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion oldExistingADV = Jsons.clone(actorDefinitionVersion)
          .withSpec(new ConnectorSpecification().withProtocolVersion(VALID_PROTOCOL_VERSION).withAdditionalProperty(SPEC_KEY, "existing"));

      when(actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DEV))
          .thenReturn(Optional.of(oldExistingADV));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, DEV,
              isCustomConnector, WORKSPACE_ID);
      verify(actorDefinitionVersionResolver).resolveVersionForTag(previousDefaultVersion.getActorDefinitionId(),
          ActorType.SOURCE, previousDefaultVersion.getDockerRepository(), DEV);
      verify(synchronousSchedulerClient).createGetSpecJob(dockerImage, isCustomConnector, WORKSPACE_ID);

      assertEquals(oldExistingADV.withSpec(newSpec), newVersion);

      verifyNoMoreInteractions(actorDefinitionVersionResolver, synchronousSchedulerClient);
      verifyNoInteractions(remoteDefinitionsProvider);
    }

  }

  @Nested
  class TestGetBreakingChanges {

    @Test
    void testGetBreakingChanges() throws IOException {
      final BreakingChanges sourceRegistryBreakingChanges =
          new BreakingChanges().withAdditionalProperty("1.0.0", new VersionBreakingChange().withMessage("A breaking change was made")
              .withUpgradeDeadline("2000-01-01").withMigrationDocumentationUrl("https://docs.airbyte.io/migration"));
      final ConnectorRegistrySourceDefinition sourceDefWithBreakingChanges =
          Jsons.clone(connectorRegistrySourceDefinition)
              .withReleases(new ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges));

      when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST))
          .thenReturn(Optional.of(sourceDefWithBreakingChanges));

      final List<ActorDefinitionBreakingChange> breakingChanges = actorDefinitionHandlerHelper.getBreakingChanges(actorDefinitionVersion,
          ActorType.SOURCE);
      assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(sourceDefWithBreakingChanges), breakingChanges);

      verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);

      verifyNoMoreInteractions(remoteDefinitionsProvider);
      verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
    }

  }

  @Nested
  class TestGetVersionBreakingChanges {

    @Test
    void testGetVersionBreakingChanges() throws IOException {
      final List<ActorDefinitionBreakingChange> breakingChangeList = List.of(
          new ActorDefinitionBreakingChange()
              .withActorDefinitionId(ACTOR_DEFINITION_ID)
              .withMigrationDocumentationUrl("https://docs.airbyte.io/2")
              .withVersion(new Version("2.0.0"))
              .withUpgradeDeadline("2023-01-01")
              .withMessage("This is a breaking change")
              .withDeadlineAction("auto_upgrade"),
          new ActorDefinitionBreakingChange()
              .withActorDefinitionId(ACTOR_DEFINITION_ID)
              .withMigrationDocumentationUrl("https://docs.airbyte.io/3")
              .withVersion(new Version("3.0.0"))
              .withUpgradeDeadline("2023-05-01")
              .withMessage("This is another breaking change"));

      when(actorDefinitionService.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion)).thenReturn(breakingChangeList);

      final ActorDefinitionVersionBreakingChanges expected = new ActorDefinitionVersionBreakingChanges()
          .minUpgradeDeadline(LocalDate.parse("2023-01-01"))
          .deadlineAction(DeadlineAction.AUTO_UPGRADE)
          .upcomingBreakingChanges(List.of(
              new io.airbyte.api.model.generated.ActorDefinitionBreakingChange()
                  .migrationDocumentationUrl("https://docs.airbyte.io/2")
                  .version("2.0.0")
                  .upgradeDeadline(LocalDate.parse(("2023-01-01")))
                  .message("This is a breaking change")
                  .deadlineAction(DeadlineAction.AUTO_UPGRADE),
              new io.airbyte.api.model.generated.ActorDefinitionBreakingChange()
                  .migrationDocumentationUrl("https://docs.airbyte.io/3")
                  .version("3.0.0")
                  .upgradeDeadline(LocalDate.parse("2023-05-01"))
                  .message("This is another breaking change")
                  .deadlineAction(DeadlineAction.DISABLE)));

      final Optional<ActorDefinitionVersionBreakingChanges> actual = actorDefinitionHandlerHelper.getVersionBreakingChanges(actorDefinitionVersion);
      assertEquals(expected, actual.orElseThrow());
    }

    @Test
    void testGetVersionBreakingChangesNoBreakingChanges() throws IOException {
      when(actorDefinitionService.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion)).thenReturn(List.of());

      final Optional<ActorDefinitionVersionBreakingChanges> actual = actorDefinitionHandlerHelper.getVersionBreakingChanges(actorDefinitionVersion);
      assertEquals(Optional.empty(), actual);
    }

  }

  @Test
  void testGetNoBreakingChangesAvailable() throws IOException {
    when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST))
        .thenReturn(Optional.of(connectorRegistrySourceDefinition));

    final List<ActorDefinitionBreakingChange> breakingChanges = actorDefinitionHandlerHelper.getBreakingChanges(actorDefinitionVersion,
        ActorType.SOURCE);
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(connectorRegistrySourceDefinition), breakingChanges);

    verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);

    verifyNoMoreInteractions(remoteDefinitionsProvider);
    verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
  }

  @Test
  void testGetBreakingChangesIfDefinitionNotFound() throws IOException {
    when(remoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)).thenReturn(Optional.empty());

    final List<ActorDefinitionBreakingChange> breakingChanges = actorDefinitionHandlerHelper.getBreakingChanges(actorDefinitionVersion,
        ActorType.SOURCE);
    verify(remoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, LATEST);
    assertEquals(List.of(), breakingChanges);

    verifyNoMoreInteractions(remoteDefinitionsProvider);
    verifyNoInteractions(synchronousSchedulerClient, actorDefinitionVersionResolver);
  }

  private String getDockerImageForTag(final String dockerImageTag) {
    return String.format("%s:%s", DOCKER_REPOSITORY, dockerImageTag);
  }

}
