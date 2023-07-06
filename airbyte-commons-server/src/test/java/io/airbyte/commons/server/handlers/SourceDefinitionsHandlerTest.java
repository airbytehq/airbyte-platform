/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.server.services.AirbyteRemoteOssCatalog;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
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

class SourceDefinitionsHandlerTest {

  private static final String TODAY_DATE_STRING = LocalDate.now().toString();
  private static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";

  private ConfigRepository configRepository;
  private StandardSourceDefinition sourceDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private SourceDefinitionsHandler sourceDefinitionsHandler;
  private Supplier<UUID> uuidSupplier;
  private SynchronousSchedulerClient schedulerSynchronousClient;
  private AirbyteRemoteOssCatalog githubStore;
  private SourceHandler sourceHandler;
  private UUID workspaceId;
  private UUID organizationId;
  private AirbyteProtocolVersionRange protocolVersionRange;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    schedulerSynchronousClient = spy(SynchronousSchedulerClient.class);
    githubStore = mock(AirbyteRemoteOssCatalog.class);
    sourceHandler = mock(SourceHandler.class);
    workspaceId = UUID.randomUUID();
    organizationId = UUID.randomUUID();
    sourceDefinition = generateSourceDefinition();
    sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    protocolVersionRange = new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.3.0"));

    sourceDefinitionsHandler = new SourceDefinitionsHandler(configRepository, uuidSupplier, schedulerSynchronousClient, githubStore, sourceHandler,
        protocolVersionRange);
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
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("host1", "host2")))
        .withSuggestedStreams(new SuggestedStreams().withStreams(List.of("stream1", "stream2")));
  }

  @Test
  @DisplayName("listSourceDefinition should return the right list")
  void testListSourceDefinitions() throws JsonValidationException, IOException, URISyntaxException {
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
  @DisplayName("getSourceDefinitionForWorkspace should return the source if the grant exists")
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
  @DisplayName("getSourceDefinitionForScope should return the source if the grant exists")
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
  @DisplayName("createSourceDefinition should not create a sourceDefinition with an unsupported protocol version")
  void testCreateSourceDefinitionWithInvalidProtocol() throws URISyntaxException, IOException, JsonValidationException {
    final String invalidProtocol = "131.1.2";
    final ActorDefinitionVersion sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    sourceDefinitionVersion.getSpec().setProtocolVersion(invalidProtocol);
    final String imageName = sourceDefinitionVersion.getDockerRepository() + ":" + sourceDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(sourceDefinition.getSourceDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true)).thenReturn(new SynchronousResponse<>(
        sourceDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(sourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));
    final CustomSourceDefinitionCreate customCreate = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId);
    assertThrows(UnsupportedProtocolVersionException.class, () -> sourceDefinitionsHandler.createCustomSourceDefinition(customCreate));

    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true);
    verify(configRepository, never()).writeStandardSourceDefinition(any());
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition")
  void testCreateCustomSourceDefinition() throws URISyntaxException, IOException {
    final StandardSourceDefinition sourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    final String imageName = sourceDefinitionVersion.getDockerRepository() + ":" + sourceDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(sourceDefinition.getSourceDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true)).thenReturn(new SynchronousResponse<>(
        sourceDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(sourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreate = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId);

    final SourceDefinitionRead expectedRead = new SourceDefinitionRead()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreate);

    assertEquals(expectedRead, actualRead);
    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true);
    verify(configRepository).writeCustomSourceDefinitionAndDefaultVersion(
        sourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null),
        sourceDefinitionVersion
            .withVersionId(null)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withAllowedHosts(null)
            .withSuggestedStreams(null),
        workspaceId,
        ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition for a workspace and organization using scopes")
  void testCreateCustomSourceDefinitionUsingScopes() throws URISyntaxException, IOException {
    final StandardSourceDefinition sourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    final String imageName = sourceDefinitionVersion.getDockerRepository() + ":" + sourceDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(sourceDefinition.getSourceDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true)).thenReturn(new SynchronousResponse<>(
        sourceDefinitionVersion.getSpec(), SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(sourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreateForWorkspace = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);

    final SourceDefinitionRead expectedRead = new SourceDefinitionRead()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .icon(SourceDefinitionsHandler.loadIcon(sourceDefinition.getIcon()))
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final SourceDefinitionRead actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForWorkspace);

    assertEquals(expectedRead, actualRead);
    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true);
    verify(configRepository).writeCustomSourceDefinitionAndDefaultVersion(
        sourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null),
        sourceDefinitionVersion
            .withVersionId(null)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withAllowedHosts(null)
            .withSuggestedStreams(null),
        workspaceId,
        ScopeType.WORKSPACE);

    final CustomSourceDefinitionCreate customCreateForOrganization = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);

    sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForOrganization);

    verify(configRepository).writeCustomSourceDefinitionAndDefaultVersion(
        sourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null),
        sourceDefinitionVersion
            .withVersionId(null)
            .withReleaseDate(null)
            .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
            .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
            .withAllowedHosts(null)
            .withSuggestedStreams(null),
        organizationId,
        ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("createCustomSourceDefinition should not create a sourceDefinition with unsupported protocol version")
  void testCreateCustomSourceDefinitionWithInvalidProtocol() throws URISyntaxException, IOException, JsonValidationException {
    final String invalidVersion = "130.0.0";
    final StandardSourceDefinition sourceDefinition = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    sourceDefinitionVersion.getSpec().setProtocolVersion(invalidVersion);
    final String imageName = sourceDefinitionVersion.getDockerRepository() + ":" + sourceDefinitionVersion.getDockerImageTag();

    when(uuidSupplier.get()).thenReturn(sourceDefinition.getSourceDefinitionId());
    when(schedulerSynchronousClient.createGetSpecJob(imageName, true)).thenReturn(new SynchronousResponse<>(
        sourceDefinitionVersion.getSpec(),
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(sourceDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomSourceDefinitionCreate customCreate = new CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId);

    assertThrows(UnsupportedProtocolVersionException.class, () -> sourceDefinitionsHandler.createCustomSourceDefinition(customCreate));

    verify(schedulerSynchronousClient).createGetSpecJob(imageName, true);
    verify(configRepository, never()).writeCustomSourceDefinitionAndDefaultVersion(any(), any(), any(), any());
  }

  @Test
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  void testUpdateSourceDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);
    final String newDockerImageTag = "averydifferenttag";
    final String newProtocolVersion = "0.2.1";
    final SourceDefinitionRead sourceDefinitionRead = sourceDefinitionsHandler
        .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final String currentTag = sourceDefinitionRead.getDockerImageTag();
    assertNotEquals(newDockerImageTag, currentTag);

    final String newImageName = sourceDefinitionVersion.getDockerRepository() + ":" + newDockerImageTag;
    final ConnectorSpecification newSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(ImmutableMap.of("foo2", "bar2")))
        .withProtocolVersion(newProtocolVersion);
    when(schedulerSynchronousClient.createGetSpecJob(newImageName, false)).thenReturn(new SynchronousResponse<>(
        newSpec,
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    final StandardSourceDefinition updatedSource = Jsons.clone(sourceDefinition).withDefaultVersionId(null);
    final ActorDefinitionVersion updatedSourceDefVersion = generateVersionFromSourceDefinition(updatedSource)
        .withDockerImageTag(newDockerImageTag).withSpec(newSpec).withProtocolVersion(newProtocolVersion);

    final SourceDefinitionRead sourceDefinitionUpdateRead = sourceDefinitionsHandler
        .updateSourceDefinition(
            new SourceDefinitionUpdate().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()).dockerImageTag(newDockerImageTag));

    assertEquals(newDockerImageTag, sourceDefinitionUpdateRead.getDockerImageTag());
    verify(schedulerSynchronousClient).createGetSpecJob(newImageName, false);
    verify(configRepository).writeSourceDefinitionAndDefaultVersion(updatedSource, updatedSourceDefVersion);
  }

  @Test
  @DisplayName("updateSourceDefinition should not update a sourceDefinition with an invalid protocol version")
  void testUpdateSourceDefinitionWithInvalidProtocol() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);
    final String newDockerImageTag = "averydifferenttag";
    final String newProtocolVersion = "132.2.1";
    final SourceDefinitionRead sourceDefinitionRead = sourceDefinitionsHandler
        .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final String currentTag = sourceDefinitionRead.getDockerImageTag();
    assertNotEquals(newDockerImageTag, currentTag);

    final String newImageName = sourceDefinitionVersion.getDockerRepository() + ":" + newDockerImageTag;
    final ConnectorSpecification newSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(ImmutableMap.of("foo2", "bar2")))
        .withProtocolVersion(newProtocolVersion);
    when(schedulerSynchronousClient.createGetSpecJob(newImageName, false)).thenReturn(new SynchronousResponse<>(
        newSpec,
        SynchronousJobMetadata.mock(ConfigType.GET_SPEC)));

    assertThrows(UnsupportedProtocolVersionException.class, () -> sourceDefinitionsHandler
        .updateSourceDefinition(
            new SourceDefinitionUpdate().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()).dockerImageTag(newDockerImageTag)));

    verify(schedulerSynchronousClient).createGetSpecJob(newImageName, false);
    verify(configRepository, never()).writeSourceDefinitionAndDefaultVersion(any(), any());
  }

  @Test
  @DisplayName("deleteSourceDefinition should correctly delete a sourceDefinition")
  void testDeleteSourceDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId());
    final StandardSourceDefinition updatedSourceDefinition = Jsons.clone(this.sourceDefinition).withTombstone(true);
    final SourceRead source = new SourceRead();

    when(configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody))
        .thenReturn(new SourceReadList().sources(Collections.singletonList(source)));

    assertFalse(sourceDefinition.getTombstone());

    sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody);

    verify(sourceHandler).deleteSource(source);
    verify(configRepository).writeStandardSourceDefinition(updatedSourceDefinition);
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
          .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
          .withReleaseDate(TODAY_DATE_STRING)
          .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
      when(githubStore.getSourceDefinitions()).thenReturn(Collections.singletonList(registrySourceDefinition));

      final var sourceDefinitionReadList = sourceDefinitionsHandler.listLatestSourceDefinitions().getSourceDefinitions();
      assertEquals(1, sourceDefinitionReadList.size());

      final var sourceDefinitionRead = sourceDefinitionReadList.get(0);
      assertEquals(
          SourceDefinitionsHandler.buildSourceDefinitionRead(ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
              ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition)),
          sourceDefinitionRead);
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    void testHttpTimeout() throws IOException {
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
