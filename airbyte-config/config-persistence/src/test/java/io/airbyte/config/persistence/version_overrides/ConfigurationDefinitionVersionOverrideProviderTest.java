/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConfigResourceType;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigScopeType;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ScopedConfiguration;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectorVersionKey;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigurationDefinitionVersionOverrideProviderTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "2.0.2";
  private static final String DOCS_URL = "https://airbyte.io/docs/";
  private static final AllowedHosts ALLOWED_HOSTS = new AllowedHosts().withHosts(List.of("https://airbyte.io"));
  private static final SuggestedStreams SUGGESTED_STREAMS = new SuggestedStreams().withStreams(List.of("users"));
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withProtocolVersion("0.2.0")
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withProtocolVersion("0.2.0")
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "theSpec", "goesHere")));
  private static final NormalizationDestinationDefinitionConfig NORMALIZATION_CONFIG = new NormalizationDestinationDefinitionConfig()
      .withNormalizationRepository("airbyte/normalization")
      .withNormalizationTag("tag")
      .withNormalizationIntegrationType("bigquery");
  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC)
      .withProtocolVersion(SPEC.getProtocolVersion())
      .withDocumentationUrl(DOCS_URL)
      .withReleaseStage(ReleaseStage.BETA)
      .withSuggestedStreams(SUGGESTED_STREAMS)
      .withAllowedHosts(ALLOWED_HOSTS)
      .withSupportsDbt(true)
      .withNormalizationConfig(NORMALIZATION_CONFIG);
  private static final ActorDefinitionVersion OVERRIDE_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2)
      .withProtocolVersion(SPEC_2.getProtocolVersion())
      .withDocumentationUrl(DOCS_URL)
      .withReleaseStage(ReleaseStage.BETA)
      .withSuggestedStreams(SUGGESTED_STREAMS)
      .withAllowedHosts(ALLOWED_HOSTS)
      .withSupportsDbt(true)
      .withNormalizationConfig(NORMALIZATION_CONFIG);

  private WorkspaceService mWorkspaceService;
  private ActorDefinitionService mActorDefinitionService;
  private ScopedConfigurationService mScopedConfigurationService;
  private ConfigurationDefinitionVersionOverrideProvider overrideProvider;

  @BeforeEach
  void setup() throws JsonValidationException, ConfigNotFoundException, IOException {
    mWorkspaceService = mock(WorkspaceService.class);
    mActorDefinitionService = mock(ActorDefinitionService.class);
    mScopedConfigurationService = mock(ScopedConfigurationService.class);
    overrideProvider = new ConfigurationDefinitionVersionOverrideProvider(mWorkspaceService, mActorDefinitionService, mScopedConfigurationService);

    when(mWorkspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withOrganizationId(ORGANIZATION_ID));
  }

  @Test
  void testGetVersionNoOverride() {
    when(mScopedConfigurationService.getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID)))
                .thenReturn(Optional.empty());

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());

    verify(mScopedConfigurationService).getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID));

    verifyNoInteractions(mActorDefinitionService);
  }

  @Test
  void testGetVersionWithOverride() throws ConfigNotFoundException, IOException {
    final UUID versionId = UUID.randomUUID();
    final ScopedConfiguration versionConfig = new ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString());

    when(mScopedConfigurationService.getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID)))
                .thenReturn(Optional.of(versionConfig));

    when(mActorDefinitionService.getActorDefinitionVersion(versionId)).thenReturn(OVERRIDE_VERSION);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);

    assertTrue(optResult.isPresent());
    assertEquals(OVERRIDE_VERSION, optResult.get());

    verify(mScopedConfigurationService).getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID));
    verify(mActorDefinitionService).getActorDefinitionVersion(versionId);

    verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService);
  }

  @Test
  void testGetVersionWithOverrideNoActor() throws ConfigNotFoundException, IOException {
    final UUID versionId = UUID.randomUUID();
    final ScopedConfiguration versionConfig = new ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString());

    when(mScopedConfigurationService.getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID)))
                .thenReturn(Optional.of(versionConfig));

    when(mActorDefinitionService.getActorDefinitionVersion(versionId)).thenReturn(OVERRIDE_VERSION);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, null, DEFAULT_VERSION);

    assertTrue(optResult.isPresent());
    assertEquals(OVERRIDE_VERSION, optResult.get());

    verify(mScopedConfigurationService).getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID));
    verify(mActorDefinitionService).getActorDefinitionVersion(versionId);

    verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService);
  }

  @Test
  void testThrowsIfVersionIdDoesNotExist() throws ConfigNotFoundException, IOException {
    final UUID versionId = UUID.randomUUID();
    final ScopedConfiguration versionConfig = new ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withScopeType(ConfigScopeType.WORKSPACE)
        .withScopeId(WORKSPACE_ID)
        .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
        .withResourceId(ACTOR_DEFINITION_ID)
        .withValue(versionId.toString());

    when(mScopedConfigurationService.getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID)))
                .thenReturn(Optional.of(versionConfig));

    when(mActorDefinitionService.getActorDefinitionVersion(versionId))
        .thenThrow(new ConfigNotFoundException(ConfigSchema.ACTOR_DEFINITION_VERSION, versionId));

    assertThrows(RuntimeException.class,
        () -> overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION));

    verify(mScopedConfigurationService).getScopedConfiguration(ConnectorVersionKey.INSTANCE, ConfigResourceType.ACTOR_DEFINITION, ACTOR_DEFINITION_ID,
        Map.of(
            ConfigScopeType.ORGANIZATION, ORGANIZATION_ID,
            ConfigScopeType.WORKSPACE, WORKSPACE_ID,
            ConfigScopeType.ACTOR, ACTOR_ID));
    verify(mActorDefinitionService).getActorDefinitionVersion(versionId);

    verifyNoMoreInteractions(mScopedConfigurationService, mActorDefinitionService);
  }

}
