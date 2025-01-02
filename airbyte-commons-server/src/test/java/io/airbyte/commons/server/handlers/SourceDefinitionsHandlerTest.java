/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
import io.airbyte.api.problems.throwable.generated.BadRequestProblem;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AbInternal;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistryEntryMetrics;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.jooq.JSONB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SourceDefinitionsHandlerTest {

  private static final String TODAY_DATE_STRING = LocalDate.now().toString();
  private static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";
  private static final String ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/source-presto/latest/icon.svg";

  private ActorDefinitionService actorDefinitionService;
  private StandardSourceDefinition sourceDefinition;
  private StandardSourceDefinition sourceDefinitionWithOptionals;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private ActorDefinitionVersion sourceDefinitionVersionWithOptionals;
  private SourceDefinitionsHandler sourceDefinitionsHandler;
  private Supplier<UUID> uuidSupplier;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private SourceHandler sourceHandler;
  private SupportStateUpdater supportStateUpdater;
  private UUID workspaceId;
  private UUID organizationId;
  private FeatureFlagClient featureFlagClient;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator;
  private SourceService sourceService;
  private WorkspaceService workspaceService;

  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    actorDefinitionService = mock(ActorDefinitionService.class);
    uuidSupplier = mock(Supplier.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    sourceHandler = mock(SourceHandler.class);
    supportStateUpdater = mock(SupportStateUpdater.class);
    workspaceId = UUID.randomUUID();
    organizationId = UUID.randomUUID();
    sourceDefinition = generateSourceDefinition();
    sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition);
    sourceDefinitionWithOptionals = generateSourceDefinitionWithOptionals();
    sourceDefinitionVersionWithOptionals = generateSourceDefinitionVersionWithOptionals(sourceDefinitionWithOptionals);
    featureFlagClient = mock(TestClient.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    airbyteCompatibleConnectorsValidator = mock(AirbyteCompatibleConnectorsValidator.class);
    sourceService = mock(SourceService.class);
    workspaceService = mock(WorkspaceService.class);

    sourceDefinitionsHandler =
        new SourceDefinitionsHandler(
            actorDefinitionService,
            uuidSupplier,
            actorDefinitionHandlerHelper,
            remoteDefinitionsProvider,
            sourceHandler,
            supportStateUpdater,
            featureFlagClient,
            actorDefinitionVersionHelper,
            airbyteCompatibleConnectorsValidator,
            sourceService,
            workspaceService,
            apiPojoConverters);
  }

  private StandardSourceDefinition generateSourceDefinition() {
    return new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withDefaultVersionId(UUID.randomUUID())
        .withName("presto")
        .withIcon("rss.svg")
        .withIconUrl(ICON_URL)
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
        .withInternalSupportLevel(100L)
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("host1", "host2")))
        .withSuggestedStreams(new SuggestedStreams().withStreams(List.of("stream1", "stream2")))
        .withLanguage("python");
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
        .withInternalSupportLevel(100L)
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
        .withAllowedHosts(null)
        .withLanguage("manifest-only");
  }

  private StandardSourceDefinition generateSourceDefinitionWithOptionals() {
    final ConnectorRegistryEntryMetrics metrics =
        new ConnectorRegistryEntryMetrics().withAdditionalProperty("all", JSONB.valueOf("{'all': {'usage': 'high'}}"));
    return generateSourceDefinition().withMetrics(metrics);
  }

  private ActorDefinitionVersion generateSourceDefinitionVersionWithOptionals(final StandardSourceDefinition sourceDefinition) {
    return generateVersionFromSourceDefinition(sourceDefinition)
        .withCdkVersion("python:1.2.3")
        .withLastPublished(new Date());
  }

  @Test
  @DisplayName("listSourceDefinition should return the right list")
  void testListSourceDefinitions() throws IOException, URISyntaxException {
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(sourceService.listStandardSourceDefinitions(false))
        .thenReturn(Lists.newArrayList(sourceDefinition, sourceDefinition2, sourceDefinitionWithOptionals));
    when(actorDefinitionService.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId(),
        sourceDefinitionWithOptionals.getDefaultVersionId())))
            .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2, sourceDefinitionVersionWithOptionals));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .cdkVersion(null)
        .lastPublished(null)
        .metrics(null)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .cdkVersion(null)
        .lastPublished(null)
        .metrics(null)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion2.getLanguage());

    final SourceDefinitionRead expectedSourceDefinitionReadWithOpts = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinitionWithOptionals.getSourceDefinitionId())
        .name(sourceDefinitionWithOptionals.getName())
        .dockerRepository(sourceDefinitionVersionWithOptionals.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersionWithOptionals.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersionWithOptionals.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersionWithOptionals.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersionWithOptionals.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersionWithOptionals.getReleaseDate()))
        .cdkVersion(sourceDefinitionVersionWithOptionals.getCdkVersion())
        .lastPublished(apiPojoConverters.toOffsetDateTime(sourceDefinitionVersionWithOptionals.getLastPublished()))
        .metrics(sourceDefinitionWithOptionals.getMetrics())
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersionWithOptionals.getLanguage());

    final SourceDefinitionReadList actualSourceDefinitionReadList = sourceDefinitionsHandler.listSourceDefinitions();

    assertEquals(
        Lists.newArrayList(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2, expectedSourceDefinitionReadWithOpts),
        actualSourceDefinitionReadList.getSourceDefinitions());
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list")
  void testListSourceDefinitionsForWorkspace() throws IOException, URISyntaxException {
    final StandardSourceDefinition sourceDefinition2 = generateSourceDefinition();
    final ActorDefinitionVersion sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2);

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(sourceService.listPublicSourceDefinitions(false)).thenReturn(Lists.newArrayList(sourceDefinition));
    when(sourceService.listGrantedSourceDefinitions(workspaceId, false)).thenReturn(Lists.newArrayList(sourceDefinition2));
    when(actorDefinitionVersionHelper.getSourceVersions(List.of(sourceDefinition, sourceDefinition2), workspaceId))
        .thenReturn(
            Map.of(
                sourceDefinitionVersion.getActorDefinitionId(), sourceDefinitionVersion,
                sourceDefinitionVersion2.getActorDefinitionId(), sourceDefinitionVersion2));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion2.getLanguage());

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

    when(sourceService.listPublicSourceDefinitions(false)).thenReturn(Lists.newArrayList(hiddenSourceDefinition, sourceDefinition));
    when(sourceService.listGrantedSourceDefinitions(workspaceId, false)).thenReturn(Lists.newArrayList(sourceDefinition2));
    when(actorDefinitionVersionHelper.getSourceVersions(List.of(sourceDefinition, sourceDefinition2), workspaceId))
        .thenReturn(Map.of(
            sourceDefinitionVersion.getActorDefinitionId(), sourceDefinitionVersion,
            sourceDefinitionVersion2.getActorDefinitionId(), sourceDefinitionVersion2));

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

    when(sourceService.listGrantableSourceDefinitions(workspaceId, false)).thenReturn(
        Lists.newArrayList(
            Map.entry(sourceDefinition, false),
            Map.entry(sourceDefinition2, true)));
    when(actorDefinitionService.getActorDefinitionVersions(List.of(sourceDefinition.getDefaultVersionId(), sourceDefinition2.getDefaultVersionId())))
        .thenReturn(Lists.newArrayList(sourceDefinitionVersion, sourceDefinitionVersion2));

    final SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.getSourceDefinitionId())
        .name(sourceDefinition2.getName())
        .dockerRepository(sourceDefinitionVersion2.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion2.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion2.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion2.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion2.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion2.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition2.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion2.getLanguage());

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
  void testGetSourceDefinition()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId());

    final SourceDefinitionRead actualSourceDefinitionRead = sourceDefinitionsHandler.getSourceDefinition(sourceDefinitionIdRequestBody);

    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead);
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForWorkspace() throws IOException {
    when(workspaceService.workspaceCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId)).thenReturn(false);

    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId = new SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .workspaceId(workspaceId);

    assertThrows(IdNotFoundKnownException.class, () -> sourceDefinitionsHandler.getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId));
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForScope() throws IOException {
    when(actorDefinitionService.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE.value())).thenReturn(
        false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);
    assertThrows(IdNotFoundKnownException.class, () -> sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForWorkspace));

    when(actorDefinitionService.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION.value()))
        .thenReturn(
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
  void testGetDefinitionWithGrantForWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(workspaceService.workspaceCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId)).thenReturn(true);
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())).thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId = new SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .workspaceId(workspaceId);

    final SourceDefinitionRead actualSourceDefinitionRead = sourceDefinitionsHandler
        .getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId);

    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead);
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should return the source definition if the grant exists")
  void testGetDefinitionWithGrantForScope()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(actorDefinitionService.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE.value()))
        .thenReturn(true);
    when(actorDefinitionService.scopeCanUseDefinition(sourceDefinition.getSourceDefinitionId(), organizationId, ScopeType.ORGANIZATION.value()))
        .thenReturn(
            true);
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())).thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

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
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.COMMUNITY)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionRead actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreate);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId());
    verify(sourceService).writeCustomConnectorMetadata(
        newSourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null)
            .withIconUrl(null),
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
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.COMMUNITY)
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newSourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final SourceDefinitionRead actualRead =
        sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForWorkspace);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreateForWorkspace.getWorkspaceId());
    verify(sourceService).writeCustomConnectorMetadata(
        newSourceDefinition
            .withCustom(true)
            .withDefaultVersionId(null)
            .withIconUrl(null),
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
    verify(sourceService).writeCustomConnectorMetadata(newSourceDefinition.withCustom(true).withDefaultVersionId(null),
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
    verify(sourceService, never()).writeCustomConnectorMetadata(any(StandardSourceDefinition.class), any(), any(), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  void testUpdateSource()
      throws ConfigNotFoundException, IOException, JsonValidationException, URISyntaxException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));

    final String newDockerImageTag = "averydifferenttag";
    final StandardSourceDefinition updatedSource =
        Jsons.clone(sourceDefinition).withDefaultVersionId(null);
    final ActorDefinitionVersion updatedSourceDefVersion =
        generateVersionFromSourceDefinition(updatedSource)
            .withDockerImageTag(newDockerImageTag)
            .withVersionId(UUID.randomUUID());

    final StandardSourceDefinition persistedUpdatedSource =
        Jsons.clone(updatedSource).withDefaultVersionId(updatedSourceDefVersion.getVersionId());

    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition) // Call at the beginning of the method
        .thenReturn(persistedUpdatedSource); // Call after we've persisted

    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom())).thenReturn(updatedSourceDefVersion);

    final List<ActorDefinitionBreakingChange> breakingChanges = generateBreakingChangesFromSourceDefinition(updatedSource);
    when(actorDefinitionHandlerHelper.getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE)).thenReturn(breakingChanges);

    final SourceDefinitionRead sourceRead =
        sourceDefinitionsHandler.updateSourceDefinition(
            new SourceDefinitionUpdate().sourceDefinitionId(this.sourceDefinition.getSourceDefinitionId())
                .dockerImageTag(newDockerImageTag));

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(newDockerImageTag)
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    assertEquals(expectedSourceDefinitionRead, sourceRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(sourceDefinitionVersion, ActorType.SOURCE, newDockerImageTag,
        sourceDefinition.getCustom());
    verify(actorDefinitionHandlerHelper).getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE);
    verify(sourceService).writeConnectorMetadata(updatedSource, updatedSourceDefVersion, breakingChanges);
    verify(supportStateUpdater).updateSupportStatesForSourceDefinition(persistedUpdatedSource);
    verifyNoMoreInteractions(actorDefinitionHandlerHelper, supportStateUpdater);
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  void testBuildSourceDefinitionUpdateNameNonCustom() {
    final StandardSourceDefinition existingSourceDefinition = sourceDefinition;

    final SourceDefinitionUpdate sourceDefinitionUpdate = new SourceDefinitionUpdate()
        .sourceDefinitionId(existingSourceDefinition.getSourceDefinitionId())
        .name("Some name that gets ignored");

    final StandardSourceDefinition newSourceDefinition =
        sourceDefinitionsHandler.buildSourceDefinitionUpdate(existingSourceDefinition, sourceDefinitionUpdate);

    assertEquals(newSourceDefinition.getName(), existingSourceDefinition.getName());
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  void testBuildSourceDefinitionUpdateNameCustom() {
    final String NEW_NAME = "My new connector name";
    final StandardSourceDefinition existingCustomSourceDefinition = generateSourceDefinition().withCustom(true);

    final SourceDefinitionUpdate sourceDefinitionUpdate = new SourceDefinitionUpdate()
        .sourceDefinitionId(existingCustomSourceDefinition.getSourceDefinitionId())
        .name(NEW_NAME);

    final StandardSourceDefinition newSourceDefinition =
        sourceDefinitionsHandler.buildSourceDefinitionUpdate(existingCustomSourceDefinition, sourceDefinitionUpdate);

    assertEquals(newSourceDefinition.getName(), NEW_NAME);
  }

  @Test
  @DisplayName("updateSourceDefinition should not update a sourceDefinition "
      + "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error")
  void testOutOfProtocolRangeUpdateSource() throws ConfigNotFoundException, IOException,
      JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
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
    verify(sourceService, never()).writeConnectorMetadata(any(StandardSourceDefinition.class), any(), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("updateSourceDefinition should not update a sourceDefinition "
      + "if Airbyte version is unsupported")
  void testUnsupportedAirbyteVersionUpdateSource() throws ConfigNotFoundException, IOException,
      JsonValidationException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), eq("12.4.0")))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(false, ""));
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())).thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);
    final SourceDefinitionRead currentSource = sourceDefinitionsHandler
        .getSourceDefinition(
            new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final String currentTag = currentSource.getDockerImageTag();
    final String newDockerImageTag = "12.4.0";
    assertNotEquals(newDockerImageTag, currentTag);

    assertThrows(BadRequestProblem.class, () -> sourceDefinitionsHandler.updateSourceDefinition(
        new SourceDefinitionUpdate().sourceDefinitionId(this.sourceDefinition.getSourceDefinitionId())
            .dockerImageTag(newDockerImageTag)));
    verify(sourceService, never()).writeConnectorMetadata(any(StandardSourceDefinition.class), any(), any());
    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("deleteSourceDefinition should correctly delete a sourceDefinition")
  void testDeleteSourceDefinition()
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinition.getSourceDefinitionId());
    final StandardSourceDefinition updatedSourceDefinition = Jsons.clone(this.sourceDefinition).withTombstone(true);
    final SourceRead newSourceDefinition = new SourceRead();

    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(sourceHandler.listSourcesForSourceDefinition(sourceDefinitionIdRequestBody))
        .thenReturn(new SourceReadList().sources(Collections.singletonList(newSourceDefinition)));

    assertFalse(sourceDefinition.getTombstone());

    sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody);

    verify(sourceHandler).deleteSource(newSourceDefinition);
    verify(sourceService).updateStandardSourceDefinition(updatedSourceDefinition);
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create a workspace grant")
  void testGrantSourceDefinitionToWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final PrivateSourceDefinitionRead expectedPrivateSourceDefinitionRead =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true);

    final PrivateSourceDefinitionRead actualPrivateSourceDefinitionRead =
        sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(workspaceId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));

    assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead);
    verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create an organization grant")
  void testGrantSourceDefinitionToOrganization()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()))
        .thenReturn(sourceDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()))
        .thenReturn(sourceDefinitionVersion);

    final SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .name(sourceDefinition.getName())
        .dockerRepository(sourceDefinitionVersion.getDockerRepository())
        .dockerImageTag(sourceDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(sourceDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .supportLevel(SupportLevel.fromValue(sourceDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(sourceDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(sourceDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ActorDefinitionResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(sourceDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(sourceDefinitionVersion.getLanguage());

    final PrivateSourceDefinitionRead expectedPrivateSourceDefinitionRead =
        new PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true);

    final PrivateSourceDefinitionRead actualPrivateSourceDefinitionRead =
        sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(organizationId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));

    assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead);
    verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("revokeSourceDefinition should correctly delete a workspace grant and organization grant")
  void testRevokeSourceDefinition() throws IOException {
    sourceDefinitionsHandler.revokeSourceDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(workspaceId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));
    verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);

    sourceDefinitionsHandler.revokeSourceDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(sourceDefinition.getSourceDefinitionId()).scopeId(organizationId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));
    verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(sourceDefinition.getSourceDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
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
        .withAbInternal(new AbInternal().withSl(100L))
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")))
        .withLanguage("python");
    when(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(Collections.singletonList(registrySourceDefinition));

    final SourceDefinitionRead expectedRead =
        sourceDefinitionsHandler.buildSourceDefinitionRead(ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
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
          .withAbInternal(new AbInternal().withSl(100L))
          .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
          .withReleaseDate(TODAY_DATE_STRING)
          .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")))
          .withLanguage("python");
      when(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(Collections.singletonList(registrySourceDefinition));

      final var sourceDefinitionReadList = sourceDefinitionsHandler.listLatestSourceDefinitions().getSourceDefinitions();
      assertEquals(1, sourceDefinitionReadList.size());

      final var sourceDefinitionRead = sourceDefinitionReadList.get(0);
      final SourceDefinitionRead expectedRead =
          sourceDefinitionsHandler.buildSourceDefinitionRead(ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
              ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition));

      assertEquals(expectedRead, sourceDefinitionRead);
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    void testHttpTimeout() {
      when(remoteDefinitionsProvider.getSourceDefinitions()).thenThrow(new RuntimeException());
      assertEquals(0, sourceDefinitionsHandler.listLatestSourceDefinitions().getSourceDefinitions().size());
    }

  }

}
