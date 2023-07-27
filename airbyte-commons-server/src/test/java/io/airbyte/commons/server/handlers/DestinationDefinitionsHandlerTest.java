/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationDefinitionReadList;
import io.airbyte.api.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList;
import io.airbyte.api.model.generated.ReleaseStage;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DestinationDefinitionsHandlerTest {

  private static final String TODAY_DATE_STRING = LocalDate.now().toString();
  private static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";

  private ConfigRepository configRepository;
  private StandardDestinationDefinition destinationDefinition;
  private StandardDestinationDefinition destinationDefinitionWithNormalization;
  private ActorDefinitionVersion destinationDefinitionVersion;
  private ActorDefinitionVersion destinationDefinitionVersionWithNormalization;

  private DestinationDefinitionsHandler destinationDefinitionsHandler;
  private Supplier<UUID> uuidSupplier;
  private SynchronousSchedulerClient schedulerSynchronousClient;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private DestinationHandler destinationHandler;
  private UUID workspaceId;
  private UUID organizationId;
  private AirbyteProtocolVersionRange protocolVersionRange;
  private FeatureFlagClient featureFlagClient;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    destinationDefinition = generateDestinationDefinition();
    destinationDefinitionWithNormalization = generateDestinationDefinition();
    destinationDefinitionVersion = generateVersionFromDestinationDefinition(destinationDefinition);
    destinationDefinitionVersionWithNormalization = generateDestinationDefinitionVersionWithNormalization(destinationDefinitionWithNormalization);
    schedulerSynchronousClient = spy(SynchronousSchedulerClient.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    destinationHandler = mock(DestinationHandler.class);
    workspaceId = UUID.randomUUID();
    organizationId = UUID.randomUUID();
    protocolVersionRange = new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.3.0"));
    featureFlagClient = mock(TestClient.class);
    destinationDefinitionsHandler = new DestinationDefinitionsHandler(
        configRepository,
        uuidSupplier,
        schedulerSynchronousClient,
        remoteDefinitionsProvider,
        destinationHandler,
        protocolVersionRange,
        featureFlagClient);
  }

  private StandardDestinationDefinition generateDestinationDefinition() {
    return new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withDefaultVersionId(UUID.randomUUID())
        .withName("presto")
        .withIcon("http.svg")
        .withTombstone(false)
        .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
  }

  private ActorDefinitionVersion generateVersionFromDestinationDefinition(final StandardDestinationDefinition destinationDefinition) {
    final ConnectorSpecification spec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(ImmutableMap.of("foo", "bar")));

    return new ActorDefinitionVersion()
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withDockerImageTag("12.3")
        .withDockerRepository("repo")
        .withDocumentationUrl("https://hulu.com")
        .withSpec(spec)
        .withProtocolVersion("0.2.2")
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)

        .withAllowedHosts(new AllowedHosts().withHosts(List.of("host1", "host2")));
  }

  private ActorDefinitionVersion generateDestinationDefinitionVersionWithNormalization(final StandardDestinationDefinition destinationDefinition) {
    return generateVersionFromDestinationDefinition(destinationDefinition)
        .withSupportsDbt(true)
        .withNormalizationConfig(new NormalizationDestinationDefinitionConfig()
            .withNormalizationRepository("repository")
            .withNormalizationTag("dev")
            .withNormalizationIntegrationType("integration-type"));
  }

  @Test
  @DisplayName("listDestinationDefinition should return the right list")
  void testListDestinations() throws IOException, URISyntaxException {

    when(configRepository.listStandardDestinationDefinitions(false))
        .thenReturn(Lists.newArrayList(destinationDefinition, destinationDefinitionWithNormalization));
    when(configRepository.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId(), destinationDefinitionWithNormalization.getDefaultVersionId())))
            .thenReturn(Lists.newArrayList(destinationDefinitionVersion, destinationDefinitionVersionWithNormalization));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionRead expectedDestinationDefinitionRead2 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinitionWithNormalization.getDestinationDefinitionId())
        .name(destinationDefinitionWithNormalization.getName())
        .dockerRepository(destinationDefinitionVersionWithNormalization.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersionWithNormalization.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersionWithNormalization.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinitionWithNormalization.getIcon()))
        .protocolVersion(destinationDefinitionVersionWithNormalization.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersionWithNormalization.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersionWithNormalization.getReleaseDate()))
        .supportsDbt(destinationDefinitionVersionWithNormalization.getSupportsDbt())
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(true)
            .normalizationRepository(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationRepository())
            .normalizationTag(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationTag())
            .normalizationIntegrationType(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationIntegrationType()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinitionWithNormalization.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler.listDestinationDefinitions();

    assertEquals(
        Lists.newArrayList(expectedDestinationDefinitionRead1, expectedDestinationDefinitionRead2),
        actualDestinationDefinitionReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list")
  void testListDestinationDefinitionsForWorkspace() throws IOException, URISyntaxException {
    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(configRepository.listPublicDestinationDefinitions(false)).thenReturn(Lists.newArrayList(destinationDefinition));
    when(configRepository.listGrantedDestinationDefinitions(workspaceId, false))
        .thenReturn(Lists.newArrayList(destinationDefinitionWithNormalization));
    when(configRepository.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId(), destinationDefinitionWithNormalization.getDefaultVersionId())))
            .thenReturn(Lists.newArrayList(destinationDefinitionVersion, destinationDefinitionVersionWithNormalization));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionRead expectedDestinationDefinitionRead2 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinitionWithNormalization.getDestinationDefinitionId())
        .name(destinationDefinitionWithNormalization.getName())
        .dockerRepository(destinationDefinitionVersionWithNormalization.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersionWithNormalization.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersionWithNormalization.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinitionWithNormalization.getIcon()))
        .protocolVersion(destinationDefinitionVersionWithNormalization.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersionWithNormalization.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersionWithNormalization.getReleaseDate()))
        .supportsDbt(destinationDefinitionVersionWithNormalization.getSupportsDbt())
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(true)
            .normalizationRepository(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationRepository())
            .normalizationTag(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationTag())
            .normalizationIntegrationType(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationIntegrationType()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinitionWithNormalization.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        Lists.newArrayList(expectedDestinationDefinitionRead1, expectedDestinationDefinitionRead2),
        actualDestinationDefinitionReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  void testListDestinationDefinitionsForWorkspaceWithHiddenConnectors() throws IOException {
    final StandardDestinationDefinition hiddenDestinationDefinition = generateDestinationDefinition();

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(featureFlagClient.boolVariation(HideActorDefinitionFromList.INSTANCE,
        new Multi(List.of(new DestinationDefinition(hiddenDestinationDefinition.getDestinationDefinitionId()), new Workspace(workspaceId)))))
            .thenReturn(true);

    when(configRepository.listPublicDestinationDefinitions(false)).thenReturn(Lists.newArrayList(destinationDefinition, hiddenDestinationDefinition));
    when(configRepository.listGrantedDestinationDefinitions(workspaceId, false))
        .thenReturn(Lists.newArrayList(destinationDefinitionWithNormalization));
    when(configRepository.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId(), destinationDefinitionWithNormalization.getDefaultVersionId())))
            .thenReturn(Lists.newArrayList(destinationDefinitionVersion, destinationDefinitionVersionWithNormalization));

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final List<UUID> expectedIds =
        Lists.newArrayList(destinationDefinition.getDestinationDefinitionId(), destinationDefinitionWithNormalization.getDestinationDefinitionId());

    assertEquals(expectedIds.size(), actualDestinationDefinitionReadList.getDestinationDefinitions().size());
    assertTrue(expectedIds.containsAll(actualDestinationDefinitionReadList.getDestinationDefinitions().stream()
        .map(DestinationDefinitionRead::getDestinationDefinitionId).toList()));
  }

  @Test
  @DisplayName("listPrivateDestinationDefinitions should return the right list")
  void testListPrivateDestinationDefinitions() throws IOException, URISyntaxException {

    when(configRepository.listGrantableDestinationDefinitions(workspaceId, false)).thenReturn(
        Lists.newArrayList(
            Map.entry(destinationDefinition, false),
            Map.entry(destinationDefinitionWithNormalization, true)));
    when(configRepository.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId(), destinationDefinitionWithNormalization.getDefaultVersionId())))
            .thenReturn(Lists.newArrayList(destinationDefinitionVersion, destinationDefinitionVersionWithNormalization));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionRead expectedDestinationDefinitionRead2 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinitionWithNormalization.getDestinationDefinitionId())
        .name(destinationDefinitionWithNormalization.getName())
        .dockerRepository(destinationDefinitionVersionWithNormalization.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersionWithNormalization.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersionWithNormalization.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinitionWithNormalization.getIcon()))
        .protocolVersion(destinationDefinitionVersionWithNormalization.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersionWithNormalization.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersionWithNormalization.getReleaseDate()))
        .supportsDbt(destinationDefinitionVersionWithNormalization.getSupportsDbt())
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(true)
            .normalizationRepository(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationRepository())
            .normalizationTag(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationTag())
            .normalizationIntegrationType(destinationDefinitionVersionWithNormalization.getNormalizationConfig().getNormalizationIntegrationType()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinitionWithNormalization.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateDestinationDefinitionRead expectedDestinationDefinitionOptInRead1 =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead1).granted(false);

    final PrivateDestinationDefinitionRead expectedDestinationDefinitionOptInRead2 =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead2).granted(true);

    final PrivateDestinationDefinitionReadList actualDestinationDefinitionOptInReadList =
        destinationDefinitionsHandler.listPrivateDestinationDefinitions(
            new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        Lists.newArrayList(expectedDestinationDefinitionOptInRead1, expectedDestinationDefinitionOptInRead2),
        actualDestinationDefinitionOptInReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("getDestinationDefinition should return the right destination")
  void testGetDestination() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody = new DestinationDefinitionIdRequestBody()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId());

    final DestinationDefinitionRead actualDestinationDefinitionRead =
        destinationDefinitionsHandler.getDestinationDefinition(destinationDefinitionIdRequestBody);

    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead);
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForWorkspace() throws IOException {
    when(configRepository.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId)).thenReturn(false);

    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId = new DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId);

    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId));
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForScope() throws IOException {
    when(configRepository.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE.value())).thenReturn(false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);
    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForWorkspace));

    when(configRepository.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION.value())).thenReturn(false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForOrganization = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);
    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForOrganization));
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should return the destination if the grant exists")
  void testGetDefinitionWithGrantForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId)).thenReturn(true);
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId = new DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId);

    final DestinationDefinitionRead actualDestinationDefinitionRead = destinationDefinitionsHandler
        .getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId);

    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead);
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should return the destination if the grant exists")
  void testGetDefinitionWithGrantForScope() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE.value())).thenReturn(true);
    when(configRepository.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION.value())).thenReturn(true);
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);

    final DestinationDefinitionRead actualDestinationDefinitionReadForWorkspace = destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace);
    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForWorkspace);

    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForOrganization = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);

    final DestinationDefinitionRead actualDestinationDefinitionReadForOrganization = destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization);
    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForOrganization);
  }

  @Test
  @DisplayName("createDestinationDefinition should not create a destinationDefinition with unsupported protocol version")
  void testCreateDestinationDefinitionShouldCheckProtocolVersion() throws URISyntaxException, IOException, JsonValidationException {
    final String invalidProtocolVersion = "121.5.6";
    final StandardDestinationDefinition destination = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateVersionFromDestinationDefinition(destination);
    destinationDefinitionVersion.getSpec().setProtocolVersion(invalidProtocolVersion);
    final String imageName = destinationDefinitionVersion.getDockerRepository() + ":" + destinationDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(destination.getDestinationDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true, workspaceId)).thenReturn(new SynchronousResponse<>(
        destinationDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(destination.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreate = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId);

    assertThrows(UnsupportedProtocolVersionException.class, () -> destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate));

    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true, workspaceId);
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition")
  void testCreateCustomDestinationDefinition() throws URISyntaxException, IOException {
    final StandardDestinationDefinition destination = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateVersionFromDestinationDefinition(destination);
    final String imageName = destinationDefinitionVersion.getDockerRepository() + ":" + destinationDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(destination.getDestinationDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true, workspaceId)).thenReturn(new SynchronousResponse<>(
        destinationDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(destination.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreate = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId);

    final DestinationDefinitionRead expectedRead = new DestinationDefinitionRead()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(destination.getDestinationDefinitionId())
        .icon(DestinationDefinitionsHandler.loadIcon(destination.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .releaseStage(ReleaseStage.CUSTOM)
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionRead actualRead = destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate);

    assertEquals(expectedRead, actualRead);
    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true, workspaceId);
    verify(configRepository).writeCustomDestinationDefinitionAndDefaultVersion(
        destination
            .withCustom(true)
            .withDefaultVersionId(null),
        destinationDefinitionVersion
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withAllowedHosts(null),
        workspaceId,
        ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition for a workspace and organization using scopes")
  void testCreateCustomDestinationDefinitionUsingScopes() throws URISyntaxException, IOException {
    final StandardDestinationDefinition destination = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateVersionFromDestinationDefinition(destination);
    final String imageName = destinationDefinitionVersion.getDockerRepository() + ":" + destinationDefinitionVersion.getDockerImageTag();
    final UUID organizationId = UUID.randomUUID();

    when(uuidSupplier.get()).thenReturn(destination.getDestinationDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(eq(imageName), eq(true), any())).thenReturn(new SynchronousResponse<>(
        destinationDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(destination.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreateForWorkspace = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .workspaceId(workspaceId);

    final DestinationDefinitionRead expectedRead = new DestinationDefinitionRead()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(destination.getDestinationDefinitionId())
        .icon(DestinationDefinitionsHandler.loadIcon(destination.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .releaseStage(ReleaseStage.CUSTOM)
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final DestinationDefinitionRead actualRead = destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForWorkspace);

    assertEquals(expectedRead, actualRead);
    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true, workspaceId);
    verify(configRepository).writeCustomDestinationDefinitionAndDefaultVersion(
        destination
            .withCustom(true)
            .withDefaultVersionId(null),
        destinationDefinitionVersion
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withAllowedHosts(null),
        workspaceId,
        ScopeType.WORKSPACE);

    final CustomDestinationDefinitionCreate customCreateForOrganization = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);

    destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForOrganization);

    verify(configRepository).writeCustomDestinationDefinitionAndDefaultVersion(
        destination
            .withCustom(true)
            .withDefaultVersionId(null),
        destinationDefinitionVersion
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withAllowedHosts(null),
        organizationId,
        ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should not create a destinationDefinition with unsupported protocol range")
  void testCreateCustomDestinationDefinitionWithInvalidProtocol() throws URISyntaxException, IOException, JsonValidationException {
    final String invalidProtocol = "122.1.22";
    final StandardDestinationDefinition destination = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateVersionFromDestinationDefinition(destination);
    destinationDefinitionVersion.getSpec().setProtocolVersion(invalidProtocol);
    final String imageName = destinationDefinitionVersion.getDockerRepository() + ":" + destinationDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(destination.getDestinationDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true, workspaceId)).thenReturn(new SynchronousResponse<>(
        destinationDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(destination.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(destination.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destination.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreate = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId);

    assertThrows(UnsupportedProtocolVersionException.class, () -> destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate));

    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true, workspaceId);
    verify(configRepository, never()).writeCustomDestinationDefinitionAndDefaultVersion(any(), any(), any(), any());
  }

  @Test
  @DisplayName("updateDestinationDefinition should correctly update a destinationDefinition")
  void testUpdateDestination() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())).thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);
    final DestinationDefinitionRead currentDestination = destinationDefinitionsHandler
        .getDestinationDefinition(
            new DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinition.getDestinationDefinitionId()));
    final String currentTag = currentDestination.getDockerImageTag();
    final String newDockerImageTag = "averydifferenttag";
    final String newProtocolVersion = "0.2.4";
    assertNotEquals(newDockerImageTag, currentTag);
    assertNotEquals(newProtocolVersion, currentDestination.getProtocolVersion());

    final String newImageName = destinationDefinitionVersion.getDockerRepository() + ":" + newDockerImageTag;
    final ConnectorSpecification newSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(ImmutableMap.of("foo2", "bar2")))
        .withProtocolVersion(newProtocolVersion);
    when(schedulerSynchronousClient.createGetSpecJob(newImageName, false, null)).thenReturn(new SynchronousResponse<>(
        newSpec,
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final StandardDestinationDefinition updatedDestination = Jsons.clone(destinationDefinition).withDefaultVersionId(null);
    final ActorDefinitionVersion updatedDestinationDefVersion = generateVersionFromDestinationDefinition(updatedDestination)
        .withDockerImageTag(newDockerImageTag).withSpec(newSpec).withProtocolVersion(newProtocolVersion);

    final DestinationDefinitionRead destinationRead = destinationDefinitionsHandler.updateDestinationDefinition(
        new DestinationDefinitionUpdate().destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
            .dockerImageTag(newDockerImageTag));

    assertEquals(newDockerImageTag, destinationRead.getDockerImageTag());
    verify(schedulerSynchronousClient).createGetSpecJob(newImageName, false, null);
    verify(configRepository).writeDestinationDefinitionAndDefaultVersion(updatedDestination, updatedDestinationDefVersion);
  }

  @Test
  @DisplayName("updateDestinationDefinition should not update a destinationDefinition if protocol version is out of range")
  void testOutOfProtocolRangeUpdateDestination() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())).thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);
    final DestinationDefinitionRead currentDestination = destinationDefinitionsHandler
        .getDestinationDefinition(
            new DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinition.getDestinationDefinitionId()));
    final String currentTag = currentDestination.getDockerImageTag();
    final String newDockerImageTag = "averydifferenttagforprotocolversion";
    final String newProtocolVersion = "120.2.4";
    assertNotEquals(newDockerImageTag, currentTag);
    assertNotEquals(newProtocolVersion, currentDestination.getProtocolVersion());

    final String newImageName = destinationDefinitionVersion.getDockerRepository() + ":" + newDockerImageTag;
    final ConnectorSpecification newSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(ImmutableMap.of("foo2", "bar2")))
        .withProtocolVersion(newProtocolVersion);
    when(schedulerSynchronousClient.createGetSpecJob(newImageName, false, null)).thenReturn(new SynchronousResponse<>(
        newSpec,
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    assertThrows(UnsupportedProtocolVersionException.class, () -> destinationDefinitionsHandler.updateDestinationDefinition(
        new DestinationDefinitionUpdate().destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
            .dockerImageTag(newDockerImageTag)));

    verify(schedulerSynchronousClient).createGetSpecJob(newImageName, false, null);
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
  }

  @Test
  @DisplayName("deleteDestinationDefinition should correctly delete a sourceDefinition")
  void testDeleteDestinationDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody =
        new DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinition.getDestinationDefinitionId());
    final StandardDestinationDefinition updatedDestinationDefinition = Jsons.clone(this.destinationDefinition).withTombstone(true);
    final DestinationRead destination = new DestinationRead();

    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(destinationHandler.listDestinationsForDestinationDefinition(destinationDefinitionIdRequestBody))
        .thenReturn(new DestinationReadList().destinations(Collections.singletonList(destination)));

    assertFalse(destinationDefinition.getTombstone());

    destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinitionIdRequestBody);

    verify(destinationHandler).deleteDestination(destination);
    verify(configRepository).writeStandardDestinationDefinition(updatedDestinationDefinition);
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspace should correctly create a workspace grant")
  void testGrantDestinationDefinitionToWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateDestinationDefinitionRead expectedPrivateDestinationDefinitionRead =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true);

    final PrivateDestinationDefinitionRead actualPrivateDestinationDefinitionRead =
        destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(workspaceId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));

    assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead);
    verify(configRepository).writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), workspaceId, ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspaceOrOrganization should correctly create an organization grant")
  void testGrantDestinationDefinitionToOrganization() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(DestinationDefinitionsHandler.loadIcon(destinationDefinition.getIcon()))
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .supportsDbt(false)
        .normalizationConfig(new io.airbyte.api.model.generated.NormalizationDestinationDefinitionConfig().supported(false))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateDestinationDefinitionRead expectedPrivateDestinationDefinitionRead =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true);

    final PrivateDestinationDefinitionRead actualPrivateDestinationDefinitionRead =
        destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(organizationId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));

    assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead);
    verify(configRepository).writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("revokeDestinationDefinitionFromWorkspace should correctly delete a workspace grant")
  void testRevokeDestinationDefinitionFromWorkspace() throws IOException {
    destinationDefinitionsHandler.revokeDestinationDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(workspaceId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));
    verify(configRepository).deleteActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE);

    destinationDefinitionsHandler.revokeDestinationDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(organizationId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));
    verify(configRepository).deleteActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
  }

  @SuppressWarnings("TypeName")
  @Nested
  @DisplayName("listLatest")
  class listLatest {

    @Test
    @DisplayName("should return the latest list")
    void testCorrect() throws InterruptedException {
      final ConnectorRegistryDestinationDefinition registryDestinationDefinition = new ConnectorRegistryDestinationDefinition()
          .withDestinationDefinitionId(UUID.randomUUID())
          .withName("some-destination")
          .withDocumentationUrl("https://airbyte.com")
          .withDockerRepository("dockerrepo")
          .withDockerImageTag("1.2.4")
          .withIcon("dest.svg")
          .withSpec(new ConnectorSpecification().withConnectionSpecification(
              Jsons.jsonNode(ImmutableMap.of("key", "val"))))
          .withTombstone(false)
          .withProtocolVersion("0.2.2")
          .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
          .withReleaseDate(TODAY_DATE_STRING)
          .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
      when(remoteDefinitionsProvider.getDestinationDefinitions()).thenReturn(Collections.singletonList(registryDestinationDefinition));

      final var destinationDefinitionReadList = destinationDefinitionsHandler.listLatestDestinationDefinitions().getDestinationDefinitions();
      assertEquals(1, destinationDefinitionReadList.size());

      final var destinationDefinitionRead = destinationDefinitionReadList.get(0);
      assertEquals(DestinationDefinitionsHandler.buildDestinationDefinitionRead(
          ConnectorRegistryConverters.toStandardDestinationDefinition(registryDestinationDefinition),
          ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDefinition)), destinationDefinitionRead);
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    void testHttpTimeout() {
      when(remoteDefinitionsProvider.getDestinationDefinitions()).thenThrow(new RuntimeException());
      assertEquals(0, destinationDefinitionsHandler.listLatestDestinationDefinitions().getDestinationDefinitions().size());
    }

  }

}
