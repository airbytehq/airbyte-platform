/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.micronaut.http.uri.UriBuilder;
import java.io.IOException;
import java.net.URI;
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
  private UUID workspaceId;
  private UUID actorDefinitionId;
  private String dockerRepository;
  private String dockerImageTag;
  private String validProtocolVersion;
  private String invalidProtocolVersion;
  private URI documentationUrl;

  private ActorDefinitionVersion createActorDefinitionVersion() {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag(dockerImageTag)
        .withDockerRepository(dockerRepository)
        .withDocumentationUrl(documentationUrl.toString())
        .withSpec(new ConnectorSpecification().withProtocolVersion(validProtocolVersion))
        .withProtocolVersion(validProtocolVersion);
  }

  @BeforeEach
  void setUp() {
    synchronousSchedulerClient = spy(SynchronousSchedulerClient.class);
    final AirbyteProtocolVersionRange protocolVersionRange = new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.3.0"));
    actorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    actorDefinitionHandlerHelper = new ActorDefinitionHandlerHelper(synchronousSchedulerClient, protocolVersionRange, actorDefinitionVersionResolver);

    workspaceId = UUID.randomUUID();
    actorDefinitionId = UUID.randomUUID();
    dockerRepository = "source-test";
    dockerImageTag = "0.1.0";
    validProtocolVersion = "0.1.0";
    invalidProtocolVersion = "123.0.0";
    documentationUrl = UriBuilder.of("").scheme("https").host("docs.com").build();
  }

  @Nested
  class TestDefaultDefinitionVersionFromCreate {

    @Test
    @DisplayName("The ActorDefinitionVersion created fromCreate should always be custom")
    void testDefaultDefinitionVersionFromCreate() throws IOException {
      when(synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(dockerImageTag), true, workspaceId))
          .thenReturn(new SynchronousResponse<>(
              new ConnectorSpecification().withProtocolVersion(validProtocolVersion), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      final ActorDefinitionVersion expectedNewVersion = new ActorDefinitionVersion()
          .withActorDefinitionId(null)
          .withDockerImageTag(dockerImageTag)
          .withDockerRepository(dockerRepository)
          .withSpec(new ConnectorSpecification().withProtocolVersion(validProtocolVersion))
          .withDocumentationUrl(documentationUrl.toString())
          .withProtocolVersion(validProtocolVersion)
          .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM);

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(dockerRepository, dockerImageTag, documentationUrl, workspaceId);
      assertEquals(expectedNewVersion, newVersion);
    }

    @Test
    @DisplayName("Creating an ActorDefinitionVersion from create with an invalid protocol version should throw an exception")
    void testDefaultDefinitionVersionFromCreateInvalidProtocolVersionThrows() throws IOException {
      when(synchronousSchedulerClient.createGetSpecJob(getDockerImageForTag(dockerImageTag), true, workspaceId))
          .thenReturn(new SynchronousResponse<>(
              new ConnectorSpecification().withProtocolVersion(invalidProtocolVersion), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      assertThrows(UnsupportedProtocolVersionException.class,
          () -> actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(dockerRepository, dockerImageTag, documentationUrl, workspaceId));
      verify(synchronousSchedulerClient).createGetSpecJob(getDockerImageForTag(dockerImageTag), true, workspaceId);
      verifyNoMoreInteractions(synchronousSchedulerClient);
    }

  }

  @Nested
  class TestDefaultDefinitionVersionFromUpdate {

    @BeforeEach
    void setUp() throws IOException {
      // default version resolver to not have the new version already
      when(actorDefinitionVersionResolver.resolveVersionForTag(any(), any(), any(), any())).thenReturn(Optional.empty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from create with a new version gets a new spec and new protocol version")
    void testDefaultDefinitionVersionFromCreateNewVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = createActorDefinitionVersion();
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

      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);
      verifyNoMoreInteractions(synchronousSchedulerClient);

      assertNotEquals(previousDefaultVersion, newVersion);
      assertEquals(newSpec, newVersion.getSpec());
      assertEquals(newValidProtocolVersion, newVersion.getProtocolVersion());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("If the 'new' version has the same dockerImageTag, we don't attempt to fetch a new spec")
    void testDefaultDefinitionVersionFromUpdateSameVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = createActorDefinitionVersion();
      final String newDockerImageTag = previousDefaultVersion.getDockerImageTag();
      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);

      assertEquals(previousDefaultVersion, newVersion);
      verifyNoInteractions(synchronousSchedulerClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Always fetch specs for dev versions")
    void testDefaultDefinitionVersionFromUpdateSameDevVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = createActorDefinitionVersion().withDockerImageTag("dev");
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

      assertEquals(previousDefaultVersion.getDockerImageTag(), newVersion.getDockerImageTag());
      assertNotEquals(previousDefaultVersion, newVersion);
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);
      verifyNoMoreInteractions(synchronousSchedulerClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update with an invalid protocol version should throw an exception")
    void testDefaultDefinitionVersionFromUpdateInvalidProtocolVersion(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = createActorDefinitionVersion();
      final String newDockerImageTag = "newTag";
      final String newDockerImage = getDockerImageForTag(newDockerImageTag);
      when(synchronousSchedulerClient.createGetSpecJob(newDockerImage, isCustomConnector, null)).thenReturn(new SynchronousResponse<>(
          new ConnectorSpecification().withProtocolVersion(invalidProtocolVersion), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

      assertThrows(UnsupportedProtocolVersionException.class,
          () -> actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector));
      verify(synchronousSchedulerClient).createGetSpecJob(newDockerImage, isCustomConnector, null);
      verifyNoMoreInteractions(synchronousSchedulerClient);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Creating an ActorDefinitionVersion from update should return an already existing one from db/remote before creating a new one")
    void testDefaultDefinitionVersioFromUpdateVersionResolved(final boolean isCustomConnector) throws IOException {
      final ActorDefinitionVersion previousDefaultVersion = createActorDefinitionVersion();

      final String newDockerImageTag = "newTagButPreviouslyUsed";
      final ActorDefinitionVersion oldExistingADV = createActorDefinitionVersion().withDockerImageTag(newDockerImageTag)
          .withSpec(new ConnectorSpecification().withProtocolVersion(validProtocolVersion).withAdditionalProperty("Something", "existing"));

      when(actorDefinitionVersionResolver.resolveVersionForTag(actorDefinitionId, ActorType.SOURCE, dockerRepository, newDockerImageTag))
          .thenReturn(Optional.of(oldExistingADV));

      final ActorDefinitionVersion newVersion =
          actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(previousDefaultVersion, ActorType.SOURCE, newDockerImageTag,
              isCustomConnector);
      assertEquals(oldExistingADV, newVersion);
      verifyNoInteractions(synchronousSchedulerClient);
    }

  }

  private String getDockerImageForTag(final String dockerImageTag) {
    return String.format("%s:%s", dockerRepository, dockerImageTag);
  }

}
