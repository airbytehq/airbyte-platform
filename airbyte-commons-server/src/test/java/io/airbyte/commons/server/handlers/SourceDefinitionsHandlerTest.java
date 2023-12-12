/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead;
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList;
import io.airbyte.api.model.generated.ReleaseStage;
import io.airbyte.api.model.generated.SourceDefinitionCreate;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SupportLevel;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RunSupportStateUpdater;
import io.airbyte.featureflag.SourceDefinition;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SourceDefinitionsHandlerTest {

  private static final String TODAY_DATE_STRING = LocalDate.now().toString();
  private static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";

  private ConfigRepository configRepository;
  private StandardSourceDefinition sourceDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private SourceDefinitionsHandler sourceDefinitionsHandler;
  private Supplier<UUID> uuidSupplier;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private SourceHandler sourceHandler;
  private SupportStateUpdater supportStateUpdater;
  private UUID workspaceId;
  private UUID organizationId;
  private FeatureFlagClient featureFlagClient;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    sourceHandler = mock(SourceHandler.class);
    supportStateUpdater = mock(SupportStateUpdater.class);
    workspaceId = UUID.randomUUID();
    organizationId = UUID.randomUUID();
    sourceDefinition = generateSourceDefinition();
    sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    featureFlagClient = mock(TestClient.class);
    sourceDefinitionsHandler =
        new SourceDefinitionsHandler(configRepository, uuidSupplier, actorDefinitionHandlerHelper, remoteDefinitionsProvider, sourceHandler,
            supportStateUpdater, featureFlagClient);
  }

  private StandardSourceDefinition generateSourceDefinition() {
    return new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withDefaultVersionId(UUID.randomUUID())
        .withName("presto")
        .withIcon("rss.svg")
        .withTombstone(false)
        .withResourceRequirements(new ActorDefinitionResourceRequirements()
            .withDefault(new ResourceRequirements().withCpuRequest("2")));
  }

  private ActorDefinitionVersion generateVersionFromSourceDefinition(final StandardSourceDefinition sourceDefinition) {
    final ConnectorSpecification spec = new ConnectorSpecification().withConnectionSpecification(
        Jsons.jsonNode(ImmutableMap.of("foo", "bar")));

    return new ActorDefinitionVersion()
        .withVersionId(sourceDefinition.getDefaultVersionId())
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withDocumentationUrl("https://netflix.com")
        .withDockerRepository("dockerstuff")
        .withDockerImageTag("12.3")
        .withSpec(spec)
        .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("host1", "host2")))
        .withSuggestedStreams(new SuggestedStreams().withStreams(List.of("stream1", "stream2")));
  }

  private List<ActorDefinitionBreakingChange> generateBreakingChangesFromSourceDefinition(final StandardSourceDefinition sourceDefinition) {
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersion(new Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21");
    return List.of(breakingChange);
  }

  private ActorDefinitionVersion generateCustomVersionFromSourceDefinition(final StandardSourceDefinition sourceDefinition) {
    return generateVersionFromSourceDefinition(sourceDefinition)
        .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
        .withReleaseDate(null)
        .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
        .withAllowedHosts(null);
  }

  @Test
  @DisplayName("listSourceDefinition should return the right list")
  void testListSourceDefinitions() throws IOException, URISyntaxException {
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(configRepository.listStandardSourceDefinitions(false)).thenReturn(Lists.newArrayList(sourceDefinition, sourceDefinition2));
    when(configRepository.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId())))
        .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionReadList actualSourceDefinitionReadList = sourceDefinitionsHandler.listSourceDefinitions();

    assertEquals(
        Lists.newArrayList(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
        actualSourceDefinitionReadList.getSourceDefinitions());
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list")
  void testListSourceDefinitionsForWorkspace() throws IOException, URISyntaxException {
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(configRepository.listPublicSourceDefinitions(false)).thenReturn(Lists.newArrayList(sourceDefinition));
    when(configRepository.listGrantedSourceDefinitions(workspaceId, false)).thenReturn(Lists.newArrayList(sourceDefinition2));
    when(configRepository.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId())))
        .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionReadList actualSourceDefinitionReadList =
        sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        Lists.newArrayList(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
        actualSourceDefinitionReadList.getSourceDefinitions());
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  void testListSourceDefinitionsForWorkspaceWithHiddenConnectors() throws IOException {
    final StandardSourceDefinition hiddenSourceDefinition = generateSourceDefinition();
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(featureFlagClient.boolVariation(HideActorDefinitionFromList.INSTANCE,
        new Multi(List.of(new SourceDefinition(hiddenSourceDefinition.getSourceDefinitionId()), new Workspace(workspaceId))))).thenReturn(true);

    when(configRepository.listPublicSourceDefinitions(false)).thenReturn(Lists.newArrayList(hiddenSourceDefinition, sourceDefinition));
    when(configRepository.listGrantedSourceDefinitions(workspaceId, false)).thenReturn(Lists.newArrayList(sourceDefinition2));
    when(configRepository.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId())))
        .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2));

    final SourceDefinitionReadList actualSourceDefinitionReadList =
        sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final List<UUID> expectedIds = Lists.newArrayList(sourceDefinition.getSourceDefinitionId(), sourceDefinition2.getSourceDefinitionId());
    assertEquals(expectedIds.size(), actualSourceDefinitionReadList.getSourceDefinitions().size());
    assertTrue(expectedIds.containsAll(actualSourceDefinitionReadList.getSourceDefinitions().stream()
        .map(SourceDefinitionRead::getSourceDefinitionId)
        .toList()));
  }

  @Test
  @DisplayName("listPrivateSourceDefinitions should return the right list")
  void testListPrivateSourceDefinitions() throws IOException, URISyntaxException {
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(configRepository.listGrantableSourceDefinitions(workspaceId, false)).thenReturn(
        Lists.newArrayList(
            Map.entry(sourceDefinition, false),
            Map.entry(sourceDefinition2, true)));
    when(configRepository.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId())))
        .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateSourceDefinitionRead expectedSourceDefinitionOptInRead1 =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead1).granted(false);

    final PrivateSourceDefinitionRead expectedSourceDefinitionOptInRead2 =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead2).granted(true);

    final PrivateSourceDefinitionReadList actualSourceDefinitionOptInReadList = sourceDefinitionsHandler.listPrivateSourceDefinitions(
        new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        Lists.newArrayList(expectedSourceDefinitionOptInRead1, expectedSourceDefinitionOptInRead2),
        actualSourceDefinitionOptInReadList.getSourceDefinitions());
  }

  @Test
  @DisplayName("getSourceDefinition should return the right source")
  void testGetSourceDefinition() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId());

    final SourceDefinitionRead actualSourceDefinitionRead = sourceDefinitionsHandler.getSourceDefinition(sourceDefinitionIdRequestBody);

    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead);
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForWorkspace() throws IOException {
    when(configRepository.workspaceCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId)).thenReturn(false);

    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId = new SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .workspaceId(workspaceId);

    assertThrows(IdNotFoundKnownException.class, () -> sourceDefinitionsHandler.getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId));
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForScope() throws IOException {
    when(configRepository.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE.value())).thenReturn(
        false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);
    assertThrows(IdNotFoundKnownException.class, () -> sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForWorkspace));

    when(configRepository.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION.value())).thenReturn(
        false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForOrganization = new ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);
    assertThrows(IdNotFoundKnownException.class,
        () -> sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForOrganization));
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should return the source definition if the grant exists")
  void testGetDefinitionWithGrantForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.workspaceCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId)).thenReturn(true);
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())).thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId = new SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .workspaceId(workspaceId);

    final SourceDefinitionRead actualSourceDefinitionRead = sourceDefinitionsHandler
        .getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId);

    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead);
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should return the source definition if the grant exists")
  void testGetDefinitionWithGrantForScope() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE.value())).thenReturn(true);
    when(configRepository.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION.value())).thenReturn(
        true);
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())).thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);

    final SourceDefinitionRead actualSourceDefinitionReadForWorkspace = sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace);
    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForWorkspace);

    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForOrganization = new ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);

    final SourceDefinitionRead actualSourceDefinitionReadForOrganization = sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization);
    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForOrganization);
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition")
  void testCreateCustomSourceDefinition() throws URISyntaxException, IOException {
    final StandardSourceDefinition newSourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition);

    when(uuidSupplier.get()).thenReturn(newSourceDefinition.getSourceDefinitionId());

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(newSourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(newSourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreate = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId()))
            .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedRead = new SourceDefinitionRead()
        .name(newSourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .sourceDefinitionId(newSourceDefinition.getSourceDefinitionId())
        .icon(SourceDefinitionsHandler.loadIcon(newSourceDefinition.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.COMMUNITY)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreate);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId());
    verify(configRepository).writeCustomConnectorMetadata(
        newSourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null),
        sourceDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE);

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition for a workspace and organization using scopes")
  void testCreateCustomSourceDefinitionUsingScopes() throws URISyntaxException, IOException {
    final StandardSourceDefinition newSourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition);

    when(uuidSupplier.get()).thenReturn(newSourceDefinition.getSourceDefinitionId());

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(newSourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(newSourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreateForWorkspace = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .workspaceId(workspaceId);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreateForWorkspace.getWorkspaceId()))
            .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedRead = new SourceDefinitionRead()
        .name(newSourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .sourceDefinitionId(newSourceDefinition.getSourceDefinitionId())
        .icon(SourceDefinitionsHandler.loadIcon(newSourceDefinition.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.COMMUNITY)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead actualRead =
        sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForWorkspace);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreateForWorkspace.getWorkspaceId());
    verify(configRepository).writeCustomConnectorMetadata(
        newSourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null),
        sourceDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE);

    final UUID organizationId = UUID.randomUUID();

    final CustomSourceDefinitionCreate customCreateForOrganization = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        null))
            .thenReturn(sourceDefinitionVersion);

    sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForOrganization);

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        null);
    verify(configRepository).writeCustomConnectorMetadata(newSourceDefinition.withCustom(true).withDefaultVersionId(null),
        sourceDefinitionVersion, organizationId, ScopeType.ORGANIZATION);

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("createCustomSourceDefinition should not create a sourceDefinition "
      + "if defaultDefinitionVersionFromCreate throws unsupported protocol version error")
  void testCreateCustomSourceDefinitionShouldCheckProtocolVersion() throws URISyntaxException, IOException {
    final StandardSourceDefinition newSourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateVersionFromSourceDefinition(newSourceDefinition);

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(newSourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(newSourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreate = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId())).thenThrow(UnsupportedProtocolVersionException.class);
    assertThrows(UnsupportedProtocolVersionException.class, () -> sourceDefinitionsHandler.createCustomSourceDefinition(customCreate));

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId());
    verify(configRepository, never()).writeCustomConnectorMetadata(any(StandardSourceDefinition.class), any(), any(), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  void testUpdateSource(final boolean runSupportStateUpdaterFlagValue) throws ConfigNotFoundException, IOException, JsonValidationException {
    when(featureFlagClient.boolVariation(RunSupportStateUpdater.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(runSupportStateUpdaterFlagValue);

    final String newDockerImageTag = "averydifferenttag";
    final StandardSourceDefinition updatedSource =
        Jsons.clone(sourceDefinition).withDefaultVersionId(null);
    final ActorDefinitionVersion updatedSourceDefVersion =
        generateVersionFromSourceDefinition(updatedSource)
            .withDockerImageTag(newDockerImageTag)
            .withVersionId(UUID.randomUUID());

    final StandardSourceDefinition persistedUpdatedSource =
        Jsons.clone(updatedSource).withDefaultVersionId(updatedSourceDefVersion.getVersionId());

    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition) // Call at the beginning of the method
        .thenReturn(persistedUpdatedSource); // Call after we've persisted

    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom())).thenReturn(updatedSourceDefVersion);

    final List<ActorDefinitionBreakingChange> breakingChanges = generateBreakingChangesFromSourceDefinition(updatedSource);
    when(actorDefinitionHandlerHelper.getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE)).thenReturn(breakingChanges);

    final SourceDefinitionRead sourceRead =
        sourceDefinitionsHandler.updateSourceDefinition(
            new SourceDefinitionUpdate().sourceDefinitionId(this.sourceDefinition.getSourceDefinitionId())
                .dockerImageTag(newDockerImageTag));

    assertEquals(newDockerImageTag, sourceRead.getDockerImageTag());
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom());
    verify(actorDefinitionHandlerHelper).getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE);
    verify(configRepository).writeConnectorMetadata(updatedSource, updatedSourceDefVersion, breakingChanges);
    if (runSupportStateUpdaterFlagValue) {
      verify(supportStateUpdater).updateSupportStatesForSourceDefinition(persistedUpdatedSource);
    } else {
      verifyNoInteractions(supportStateUpdater);
    }
    verifyNoMoreInteractions(actorDefinitionHandlerHelper, supportStateUpdater);
  }

  @Test
  @DisplayName("updateSourceDefinition should not update a sourceDefinition "
      + "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error")
  void testOutOfProtocolRangeUpdateSource() throws ConfigNotFoundException, IOException,
      JsonValidationException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);
    final SourceDefinitionRead currentSource = sourceDefinitionsHandler
        .getSourceDefinition(
            new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final String currentTag = currentSource.getDockerImageTag();
    final String newDockerImageTag = "averydifferenttagforprotocolversion";
    assertNotEquals(newDockerImageTag, currentTag);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom()))
            .thenThrow(UnsupportedProtocolVersionException.class);

    assertThrows(UnsupportedProtocolVersionException.class, () -> sourceDefinitionsHandler.updateSourceDefinition(
        new SourceDefinitionUpdate().sourceDefinitionId(this.sourceDefinition.getSourceDefinitionId())
            .dockerImageTag(newDockerImageTag)));

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom());
    verify(configRepository, never()).writeConnectorMetadata(any(StandardSourceDefinition.class), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("deleteSourceDefinition should correctly delete a sourceDefinition")
  void testDeleteSourceDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId());
    final StandardSourceDefinition updatedSourceDefinition = Jsons.clone(this.sourceDefinition).withTombstone(true);
    final SourceRead newSourceDefinition = new SourceRead();

    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody))
        .thenReturn(new SourceReadList().sources(Collections.singletonList(newSourceDefinition)));

    assertFalse(sourceDefinition.getTombstone());

    sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody);

    verify(sourceHandler).deleteSource(newSourceDefinition);
    verify(configRepository).updateStandardSourceDefinition(updatedSourceDefinition);
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create a workspace grant")
  void testGrantSourceDefinitionToWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateSourceDefinitionRead expectedPrivateSourceDefinitionRead =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true);

    final PrivateSourceDefinitionRead actualPrivateSourceDefinitionRead =
        sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(workspaceId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));

    assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead);
    verify(configRepository).writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create an organization grant")
  void testGrantSourceDefinitionToOrganization() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final PrivateSourceDefinitionRead expectedPrivateSourceDefinitionRead =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true);

    final PrivateSourceDefinitionRead actualPrivateSourceDefinitionRead =
        sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(organizationId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));

    assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead);
    verify(configRepository).writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("revokeSourceDefinition should correctly delete a workspace grant and organization grant")
  void testRevokeSourceDefinition() throws IOException {
    sourceDefinitionsHandler.revokeSourceDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(workspaceId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));
    verify(configRepository).deleteActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);

    sourceDefinitionsHandler.revokeSourceDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(organizationId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));
    verify(configRepository).deleteActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("should transform support level none to none")
  void testNoneSupportLevel() {
    final ConnectorRegistrySourceDefinition registrySourceDefinition = new ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("some-source")
        .withDocumentationUrl("https://airbyte.com")
        .withDockerRepository("dockerrepo")
        .withDockerImageTag("1.2.4")
        .withIcon("source.svg")
        .withSpec(new ConnectorSpecification().withConnectionSpecification(
            Jsons.jsonNode(ImmutableMap.of("key", "val"))))
        .withTombstone(false)
        .withSupportLevel(io.airbyte.config.SupportLevel.NONE)
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
    when(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(Collections.singletonList(registrySourceDefinition));

    final SourceDefinitionRead expectedRead =
        SourceDefinitionsHandler.buildSourceDefinitionRead(ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
            ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition));

    assertEquals(expectedRead.getSupportLevel(), SupportLevel.NONE);
  }

  @SuppressWarnings("TypeName")
  @Nested
  @DisplayName("listLatest")
  class listLatest {

    @Test
    @DisplayName("should return the latest list")
    void testCorrect() {
      final ConnectorRegistrySourceDefinition registrySourceDefinition = new ConnectorRegistrySourceDefinition()
          .withSourceDefinitionId(UUID.randomUUID())
          .withName("some-source")
          .withDocumentationUrl("https://airbyte.com")
          .withDockerRepository("dockerrepo")
          .withDockerImageTag("1.2.4")
          .withIcon("source.svg")
          .withSpec(new ConnectorSpecification().withConnectionSpecification(
              Jsons.jsonNode(ImmutableMap.of("key", "val"))))
          .withTombstone(false)
          .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
          .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
          .withReleaseDate(TODAY_DATE_STRING)
          .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
      when(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(Collections.singletonList(registrySourceDefinition));

      final var sourceDefinitionReadList = sourceDefinitionsHandler.listLatestSourceDefinitions().getSourceDefinitions();
      assertEquals(1, sourceDefinitionReadList.size());

      final var sourceDefinitionRead = sourceDefinitionReadList.get(0);
      final SourceDefinitionRead expectedRead =
          SourceDefinitionsHandler.buildSourceDefinitionRead(ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
              ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition));

      assertEquals(expectedRead, sourceDefinitionRead);
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    void testHttpTimeout() {
      when(remoteDefinitionsProvider.getSourceDefinitions()).thenThrow(new RuntimeException());
      assertEquals(0, sourceDefinitionsHandler.listLatestSourceDefinitions().getSourceDefinitions().size());
    }

    @Test
    @DisplayName("Icon should be an SVG icon")
    void testIconHoldsData() {
      final String icon = SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon());
      assertNotNull(icon);
      assertTrue(icon.contains("<svg"));
    }

  }

}
