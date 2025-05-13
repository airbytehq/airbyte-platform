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

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationDefinitionReadList;
import io.airbyte.api.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList;
import io.airbyte.api.model.generated.ReleaseStage;
import io.airbyte.api.model.generated.SupportLevel;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.problems.throwable.generated.BadRequestProblem;
import io.airbyte.commons.entitlements.Entitlement;
import io.airbyte.commons.entitlements.LicenseEntitlementChecker;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AbInternal;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistryEntryMetrics;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.ScopedResourceRequirements;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator;
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult;
import io.airbyte.config.init.SupportStateUpdater;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HideActorDefinitionFromList;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
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

class DestinationDefinitionsHandlerTest {

  private static final String TODAY_DATE_STRING = LocalDate.now().toString();
  private static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";
  private static final String ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-presto/latest/icon.svg";

  private ActorDefinitionService actorDefinitionService;

  private StandardDestinationDefinition destinationDefinition;
  private StandardDestinationDefinition destinationDefinitionWithOptionals;

  private ActorDefinitionVersion destinationDefinitionVersion;
  private ActorDefinitionVersion destinationDefinitionVersionWithOptionals;

  private DestinationDefinitionsHandler destinationDefinitionsHandler;
  private Supplier<UUID> uuidSupplier;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private RemoteDefinitionsProvider remoteDefinitionsProvider;
  private DestinationHandler destinationHandler;
  private SupportStateUpdater supportStateUpdater;
  private UUID workspaceId;
  private UUID organizationId;
  private FeatureFlagClient featureFlagClient;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private AirbyteCompatibleConnectorsValidator airbyteCompatibleConnectorsValidator;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;
  private LicenseEntitlementChecker licenseEntitlementChecker;
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    destinationService = mock(DestinationService.class);
    workspaceService = mock(WorkspaceService.class);
    actorDefinitionService = mock(ActorDefinitionService.class);
    uuidSupplier = mock(Supplier.class);
    destinationDefinition = generateDestinationDefinition();
    destinationDefinitionWithOptionals = generateDestinationDefinitionWithOptionals();
    destinationDefinitionVersion = generateVersionFromDestinationDefinition(destinationDefinition);
    destinationDefinitionVersionWithOptionals = generateDestinationDefinitionVersionWithOptionals(destinationDefinitionWithOptionals);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    remoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    licenseEntitlementChecker = mock(LicenseEntitlementChecker.class);
    destinationHandler = mock(DestinationHandler.class);
    supportStateUpdater = mock(SupportStateUpdater.class);
    workspaceId = UUID.randomUUID();
    organizationId = UUID.randomUUID();
    featureFlagClient = mock(TestClient.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    airbyteCompatibleConnectorsValidator = mock(AirbyteCompatibleConnectorsValidator.class);
    destinationDefinitionsHandler = new DestinationDefinitionsHandler(
        actorDefinitionService,
        uuidSupplier,
        actorDefinitionHandlerHelper,
        remoteDefinitionsProvider,
        destinationHandler,
        supportStateUpdater,
        featureFlagClient,
        actorDefinitionVersionHelper,
        airbyteCompatibleConnectorsValidator,
        destinationService,
        workspaceService,
        licenseEntitlementChecker,
        apiPojoConverters);
  }

  private StandardDestinationDefinition generateDestinationDefinition() {
    return new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withDefaultVersionId(UUID.randomUUID())
        .withName("presto")
        .withIcon("http.svg")
        .withIconUrl(ICON_URL)
        .withTombstone(false)
        .withResourceRequirements(new ScopedResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
  }

  private ActorDefinitionVersion generateVersionFromDestinationDefinition(final StandardDestinationDefinition destinationDefinition) {
    final ConnectorSpecification spec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(Map.of("foo", "bar")));

    return new ActorDefinitionVersion()
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withDockerImageTag("12.3")
        .withDockerRepository("repo")
        .withDocumentationUrl("https://hulu.com")
        .withSpec(spec)
        .withProtocolVersion("0.2.2")
        .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
        .withReleaseDate(TODAY_DATE_STRING)
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("host1", "host2")))
        .withLanguage("java")
        .withSupportsDataActivation(false);
  }

  private List<ActorDefinitionBreakingChange> generateBreakingChangesFromDestinationDefinition(final StandardDestinationDefinition destDef) {
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(destDef.getDestinationDefinitionId())
        .withVersion(new Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21");
    return List.of(breakingChange);
  }

  private ActorDefinitionVersion generateCustomVersionFromDestinationDefinition(final StandardDestinationDefinition destinationDefinition) {
    return generateVersionFromDestinationDefinition(destinationDefinition)
        .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
        .withReleaseDate(null)
        .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withReleaseStage(io.airbyte.config.ReleaseStage.CUSTOM)
        .withAllowedHosts(null)
        .withLanguage("manifest-only");
  }

  private StandardDestinationDefinition generateDestinationDefinitionWithOptionals() {
    final ConnectorRegistryEntryMetrics metrics =
        new ConnectorRegistryEntryMetrics().withAdditionalProperty("all", JSONB.valueOf("{'all': {'usage': 'high'}}"));
    return generateDestinationDefinition().withMetrics(metrics);
  }

  private ActorDefinitionVersion generateDestinationDefinitionVersionWithOptionals(final StandardDestinationDefinition destinationDefinition) {
    return generateVersionFromDestinationDefinition(destinationDefinition)
        .withCdkVersion("python:1.2.3")
        .withLastPublished(new Date());
  }

  @Test
  @DisplayName("listDestinationDefinition should return the right list")
  void testListDestinations() throws IOException, URISyntaxException {
    when(destinationService.listStandardDestinationDefinitions(false))
        .thenReturn(List.of(destinationDefinition, destinationDefinitionWithOptionals));
    when(actorDefinitionService.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId(),
            destinationDefinitionWithOptionals.getDefaultVersionId())))
                .thenReturn(List.of(destinationDefinitionVersion,
                    destinationDefinitionVersionWithOptionals));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionRead expectedDestinationDefinitionReadWithOpts = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinitionWithOptionals.getDestinationDefinitionId())
        .name(destinationDefinitionWithOptionals.getName())
        .dockerRepository(destinationDefinitionVersionWithOptionals.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersionWithOptionals.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersionWithOptionals.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersionWithOptionals.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersionWithOptionals.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersionWithOptionals.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersionWithOptionals.getReleaseDate()))
        .cdkVersion(destinationDefinitionVersionWithOptionals.getCdkVersion())
        .lastPublished(apiPojoConverters.toOffsetDateTime(destinationDefinitionVersionWithOptionals.getLastPublished()))
        .metrics(destinationDefinitionWithOptionals.getMetrics())
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinitionWithOptionals.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersionWithOptionals.getLanguage())
        .supportsDataActivation(destinationDefinitionVersionWithOptionals.getSupportsDataActivation());

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler.listDestinationDefinitions();

    assertEquals(
        List.of(expectedDestinationDefinitionRead1, expectedDestinationDefinitionReadWithOpts),
        actualDestinationDefinitionReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list")
  void testListDestinationDefinitionsForWorkspace() throws IOException, URISyntaxException, ConfigNotFoundException, JsonValidationException {
    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(mock(StandardWorkspace.class));
    when(destinationService.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition));
    when(actorDefinitionVersionHelper.getDestinationVersions(List.of(destinationDefinition), workspaceId))
        .thenReturn(Map.of(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion));
    when(licenseEntitlementChecker.checkEntitlements(any(), eq(Entitlement.DESTINATION_CONNECTOR),
        eq(List.of(destinationDefinition.getDestinationDefinitionId()))))
            .thenReturn(Map.of(destinationDefinition.getDestinationDefinitionId(), true));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        List.of(expectedDestinationDefinitionRead1),
        actualDestinationDefinitionReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out unentitled connectors")
  void testListDestinationDefinitionsForWorkspaceWithUnentitledConnectors() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition unentitledDestinationDefinition = generateDestinationDefinition();

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(mock(StandardWorkspace.class));

    when(licenseEntitlementChecker.checkEntitlements(any(), eq(Entitlement.DESTINATION_CONNECTOR),
        eq(List.of(destinationDefinition.getDestinationDefinitionId(), unentitledDestinationDefinition.getDestinationDefinitionId()))))
            .thenReturn(Map.of(
                destinationDefinition.getDestinationDefinitionId(), true,
                unentitledDestinationDefinition.getDestinationDefinitionId(), false));

    when(destinationService.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition, unentitledDestinationDefinition));
    when(actorDefinitionVersionHelper.getDestinationVersions(List.of(destinationDefinition), workspaceId))
        .thenReturn(Map.of(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion));

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final List<UUID> expectedIds =
        List.of(destinationDefinition.getDestinationDefinitionId());

    assertEquals(expectedIds.size(), actualDestinationDefinitionReadList.getDestinationDefinitions().size());
    assertTrue(expectedIds.containsAll(actualDestinationDefinitionReadList.getDestinationDefinitions().stream()
        .map(DestinationDefinitionRead::getDestinationDefinitionId).toList()));
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  void testListDestinationDefinitionsForWorkspaceWithHiddenConnectors() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition hiddenDestinationDefinition = generateDestinationDefinition();

    when(featureFlagClient.boolVariation(eq(HideActorDefinitionFromList.INSTANCE), any())).thenReturn(false);
    when(featureFlagClient.boolVariation(HideActorDefinitionFromList.INSTANCE,
        new Multi(List.of(new DestinationDefinition(hiddenDestinationDefinition.getDestinationDefinitionId()), new Workspace(workspaceId)))))
            .thenReturn(true);

    when(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(mock(StandardWorkspace.class));
    when(licenseEntitlementChecker.checkEntitlements(any(), eq(Entitlement.DESTINATION_CONNECTOR),
        eq(List.of(destinationDefinition.getDestinationDefinitionId(), hiddenDestinationDefinition.getDestinationDefinitionId()))))
            .thenReturn(Map.of(
                destinationDefinition.getDestinationDefinitionId(), true,
                hiddenDestinationDefinition.getDestinationDefinitionId(), true));

    when(destinationService.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition, hiddenDestinationDefinition));
    when(actorDefinitionVersionHelper.getDestinationVersions(List.of(destinationDefinition), workspaceId))
        .thenReturn(Map.of(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion));

    final DestinationDefinitionReadList actualDestinationDefinitionReadList = destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId));

    final List<UUID> expectedIds =
        List.of(destinationDefinition.getDestinationDefinitionId());

    assertEquals(expectedIds.size(), actualDestinationDefinitionReadList.getDestinationDefinitions().size());
    assertTrue(expectedIds.containsAll(actualDestinationDefinitionReadList.getDestinationDefinitions().stream()
        .map(DestinationDefinitionRead::getDestinationDefinitionId).toList()));
  }

  @Test
  @DisplayName("listPrivateDestinationDefinitions should return the right list")
  void testListPrivateDestinationDefinitions() throws IOException, URISyntaxException {

    when(destinationService.listGrantableDestinationDefinitions(workspaceId, false)).thenReturn(
        List.of(Map.entry(destinationDefinition, false)));
    when(actorDefinitionService.getActorDefinitionVersions(
        List.of(destinationDefinition.getDefaultVersionId())))
            .thenReturn(List.of(destinationDefinitionVersion));

    final DestinationDefinitionRead expectedDestinationDefinitionRead1 = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final PrivateDestinationDefinitionRead expectedDestinationDefinitionOptInRead1 =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead1).granted(false);

    final PrivateDestinationDefinitionReadList actualDestinationDefinitionOptInReadList =
        destinationDefinitionsHandler.listPrivateDestinationDefinitions(
            new WorkspaceIdRequestBody().workspaceId(workspaceId));

    assertEquals(
        List.of(expectedDestinationDefinitionOptInRead1),
        actualDestinationDefinitionOptInReadList.getDestinationDefinitions());
  }

  @Test
  @DisplayName("getDestinationDefinition should return the right destination")
  void testGetDestination()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionRead actualDestinationDefinitionRead =
        destinationDefinitionsHandler.getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true);

    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead);
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForWorkspace() throws IOException {
    when(workspaceService.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId)).thenReturn(false);

    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId = new DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId);

    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId));
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should throw an exception for a missing grant")
  void testGetDefinitionWithoutGrantForScope() throws IOException {
    when(actorDefinitionService.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE.value())).thenReturn(false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForWorkspace = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE);
    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForWorkspace));

    when(actorDefinitionService.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION.value())).thenReturn(false);
    final ActorDefinitionIdWithScope actorDefinitionIdWithScopeForOrganization = new ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);
    assertThrows(IdNotFoundKnownException.class,
        () -> destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForOrganization));
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should return the destination definition if the grant exists")
  void testGetDefinitionWithGrantForWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(workspaceService.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId)).thenReturn(true);
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId = new DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId);

    final DestinationDefinitionRead actualDestinationDefinitionRead = destinationDefinitionsHandler
        .getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId);

    assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead);
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should return the destination definition if the grant exists")
  void testGetDefinitionWithGrantForScope()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(actorDefinitionService.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE.value())).thenReturn(true);
    when(actorDefinitionService.scopeCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION.value())).thenReturn(true);
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

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
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition")
  void testCreateCustomDestinationDefinition() throws URISyntaxException, IOException {
    final StandardDestinationDefinition newDestinationDefinition = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition);

    when(uuidSupplier.get()).thenReturn(newDestinationDefinition.getDestinationDefinitionId());

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreate = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId()))
            .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedRead = new DestinationDefinitionRead()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(newDestinationDefinition.getDestinationDefinitionId())
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionRead actualRead = destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId());
    verify(destinationService).writeCustomConnectorMetadata(
        newDestinationDefinition
            .withCustom(true)
            .withDefaultVersionId(null)
            .withIconUrl(null),
        destinationDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE);

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition for a workspace and organization using scopes")
  void testCreateCustomDestinationDefinitionUsingScopes() throws URISyntaxException, IOException {
    final StandardDestinationDefinition newDestinationDefinition = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition);

    when(uuidSupplier.get()).thenReturn(newDestinationDefinition.getDestinationDefinitionId());

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreateForWorkspace = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .workspaceId(null); // scopeType and scopeId should be sufficient to resolve to the expected workspaceId

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(), workspaceId)).thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedRead = new DestinationDefinitionRead()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(newDestinationDefinition.getDestinationDefinitionId())
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.CUSTOM)
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final DestinationDefinitionRead actualRead =
        destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForWorkspace);

    assertEquals(expectedRead, actualRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        workspaceId);
    verify(destinationService).writeCustomConnectorMetadata(
        newDestinationDefinition
            .withCustom(true)
            .withDefaultVersionId(null)
            .withIconUrl(null),
        destinationDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE);

    // TODO: custom connectors for organizations are not currently supported. Jobs currently require an
    // explicit workspace ID to resolve a dataplane group where the job should run. We can uncomment
    // this section of the test once we support resolving a default dataplane group for a given
    // organization ID.

    // final UUID organizationId = UUID.randomUUID();
    //
    // final CustomDestinationDefinitionCreate customCreateForOrganization = new
    // CustomDestinationDefinitionCreate()
    // .destinationDefinition(create)
    // .scopeId(organizationId)
    // .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);
    //
    // when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(),
    // create.getDockerImageTag(),
    // create.getDocumentationUrl(),
    // null))
    // .thenReturn(destinationDefinitionVersion);
    //
    // destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForOrganization);
    //
    // verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(),
    // create.getDockerImageTag(),
    // create.getDocumentationUrl(),
    // null);
    // verify(destinationService).writeCustomConnectorMetadata(newDestinationDefinition.withCustom(true).withDefaultVersionId(null),
    // destinationDefinitionVersion, organizationId, ScopeType.ORGANIZATION);

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should not create a destinationDefinition "
      + "if defaultDefinitionVersionFromCreate throws unsupported protocol version error")
  void testCreateCustomDestinationDefinitionShouldCheckProtocolVersion() throws URISyntaxException, IOException {
    final StandardDestinationDefinition newDestinationDefinition = generateDestinationDefinition();
    final ActorDefinitionVersion destinationDefinitionVersion = generateVersionFromDestinationDefinition(newDestinationDefinition);

    final DestinationDefinitionCreate create = new DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()));

    final CustomDestinationDefinitionCreate customCreate = new CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId())).thenThrow(UnsupportedProtocolVersionException.class);
    assertThrows(UnsupportedProtocolVersionException.class, () -> destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate));

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(), create.getDockerImageTag(),
        create.getDocumentationUrl(),
        customCreate.getWorkspaceId());
    verify(destinationService, never()).writeCustomConnectorMetadata(any(StandardDestinationDefinition.class), any(), any(), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("updateDestinationDefinition should correctly update a destinationDefinition")
  void testUpdateDestination()
      throws ConfigNotFoundException, IOException, JsonValidationException, URISyntaxException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));

    final String newDockerImageTag = "averydifferenttag";
    final StandardDestinationDefinition updatedDestination =
        Jsons.clone(destinationDefinition).withDefaultVersionId(null);
    final ActorDefinitionVersion updatedDestinationDefVersion =
        generateVersionFromDestinationDefinition(updatedDestination)
            .withDockerImageTag(newDockerImageTag)
            .withVersionId(UUID.randomUUID());

    final StandardDestinationDefinition persistedUpdatedDestination =
        Jsons.clone(updatedDestination).withDefaultVersionId(updatedDestinationDefVersion.getVersionId());

    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition) // Call at the beginning of the method
        .thenReturn(persistedUpdatedDestination); // Call after we've persisted

    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(destinationDefinitionVersion, ActorType.DESTINATION, newDockerImageTag,
        destinationDefinition.getCustom(), workspaceId)).thenReturn(updatedDestinationDefVersion);

    final List<ActorDefinitionBreakingChange> breakingChanges = generateBreakingChangesFromDestinationDefinition(updatedDestination);
    when(actorDefinitionHandlerHelper.getBreakingChanges(updatedDestinationDefVersion, ActorType.DESTINATION)).thenReturn(breakingChanges);

    final DestinationDefinitionRead destinationRead =
        destinationDefinitionsHandler.updateDestinationDefinition(
            new DestinationDefinitionUpdate().destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
                .dockerImageTag(newDockerImageTag).workspaceId(workspaceId));

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(newDockerImageTag)
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    assertEquals(expectedDestinationDefinitionRead, destinationRead);
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(destinationDefinitionVersion, ActorType.DESTINATION, newDockerImageTag,
        destinationDefinition.getCustom(), workspaceId);
    verify(actorDefinitionHandlerHelper).getBreakingChanges(updatedDestinationDefVersion, ActorType.DESTINATION);
    verify(destinationService).writeConnectorMetadata(updatedDestination, updatedDestinationDefVersion, breakingChanges);
    verify(supportStateUpdater).updateSupportStatesForDestinationDefinition(persistedUpdatedDestination);
    verifyNoMoreInteractions(actorDefinitionHandlerHelper, supportStateUpdater);
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  void testBuildDestinationDefinitionUpdateNameNonCustom() {
    final StandardDestinationDefinition existingDestinationDefinition = destinationDefinition;

    final DestinationDefinitionUpdate destinationDefinitionUpdate = new DestinationDefinitionUpdate()
        .destinationDefinitionId(existingDestinationDefinition.getDestinationDefinitionId())
        .name("Some name that gets ignored");

    final StandardDestinationDefinition newDestinationDefinition =
        destinationDefinitionsHandler.buildDestinationDefinitionUpdate(existingDestinationDefinition, destinationDefinitionUpdate);

    assertEquals(newDestinationDefinition.getName(), existingDestinationDefinition.getName());
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  void testBuildDestinationDefinitionUpdateNameCustom() {
    final String NEW_NAME = "My new connector name";
    final StandardDestinationDefinition existingCustomDestinationDefinition = generateDestinationDefinition().withCustom(true);

    final DestinationDefinitionUpdate destinationDefinitionUpdate = new DestinationDefinitionUpdate()
        .destinationDefinitionId(existingCustomDestinationDefinition.getDestinationDefinitionId())
        .name(NEW_NAME);

    final StandardDestinationDefinition newDestinationDefinition = destinationDefinitionsHandler.buildDestinationDefinitionUpdate(
        existingCustomDestinationDefinition, destinationDefinitionUpdate);

    assertEquals(newDestinationDefinition.getName(), NEW_NAME);
  }

  @Test
  @DisplayName("updateDestinationDefinition should not update a destinationDefinition "
      + "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error")
  void testOutOfProtocolRangeUpdateDestination() throws ConfigNotFoundException, IOException,
      JsonValidationException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), anyString()))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(true, ""));
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())).thenReturn(destinationDefinition);
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);
    final DestinationDefinitionRead currentDestination = destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true);
    final String currentTag = currentDestination.getDockerImageTag();
    final String newDockerImageTag = "averydifferenttagforprotocolversion";
    assertNotEquals(newDockerImageTag, currentTag);

    when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(destinationDefinitionVersion, ActorType.DESTINATION, newDockerImageTag,
        destinationDefinition.getCustom(), workspaceId))
            .thenThrow(UnsupportedProtocolVersionException.class);

    assertThrows(UnsupportedProtocolVersionException.class, () -> destinationDefinitionsHandler.updateDestinationDefinition(
        new DestinationDefinitionUpdate().destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
            .dockerImageTag(newDockerImageTag).workspaceId(workspaceId)));

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(destinationDefinitionVersion, ActorType.DESTINATION, newDockerImageTag,
        destinationDefinition.getCustom(), workspaceId);
    verify(destinationService, never()).writeConnectorMetadata(any(StandardDestinationDefinition.class), any(), any());

    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("updateDestinationDefinition should not update a destinationDefinition "
      + "if Airbyte version is unsupported")
  void testUnsupportedAirbyteVersionUpdateDestination() throws ConfigNotFoundException, IOException,
      JsonValidationException {
    when(airbyteCompatibleConnectorsValidator.validate(anyString(), eq("12.4.0")))
        .thenReturn(new ConnectorPlatformCompatibilityValidationResult(false, ""));
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);
    final DestinationDefinitionRead currentDestination = destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true);
    final String currentTag = currentDestination.getDockerImageTag();
    final String newDockerImageTag = "12.4.0";
    assertNotEquals(newDockerImageTag, currentTag);

    assertThrows(BadRequestProblem.class, () -> destinationDefinitionsHandler.updateDestinationDefinition(
        new DestinationDefinitionUpdate().destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
            .dockerImageTag(newDockerImageTag)));
    verify(destinationService, never()).writeConnectorMetadata(any(StandardDestinationDefinition.class), any(), any());
    verifyNoMoreInteractions(actorDefinitionHandlerHelper);
  }

  @Test
  @DisplayName("deleteDestinationDefinition should correctly delete a sourceDefinition")
  void testDeleteDestinationDefinition()
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {

    final StandardDestinationDefinition updatedDestinationDefinition = Jsons.clone(this.destinationDefinition).withTombstone(true);
    final DestinationRead newDestinationDefinition = new DestinationRead();

    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(destinationHandler.listDestinationsForDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(new DestinationReadList().destinations(Collections.singletonList(newDestinationDefinition)));

    assertFalse(destinationDefinition.getTombstone());

    destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinition.getDestinationDefinitionId());

    verify(destinationHandler).deleteDestination(newDestinationDefinition);
    verify(destinationService).updateStandardDestinationDefinition(updatedDestinationDefinition);
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspace should correctly create a workspace grant")
  void testGrantDestinationDefinitionToWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final PrivateDestinationDefinitionRead expectedPrivateDestinationDefinitionRead =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true);

    final PrivateDestinationDefinitionRead actualPrivateDestinationDefinitionRead =
        destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(workspaceId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));

    assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead);
    verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE);
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspaceOrOrganization should correctly create an organization grant")
  void testGrantDestinationDefinitionToOrganization()
      throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
        .thenReturn(destinationDefinition);
    when(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationDefinitionRead expectedDestinationDefinitionRead = new DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(new URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(SupportLevel.fromValue(destinationDefinitionVersion.getSupportLevel().value()))
        .releaseStage(ReleaseStage.fromValue(destinationDefinitionVersion.getReleaseStage().value()))
        .releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(new io.airbyte.api.model.generated.ScopedResourceRequirements()
            ._default(new io.airbyte.api.model.generated.ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()))
            .jobSpecific(Collections.emptyList()))
        .language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation());

    final PrivateDestinationDefinitionRead expectedPrivateDestinationDefinitionRead =
        new PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true);

    final PrivateDestinationDefinitionRead actualPrivateDestinationDefinitionRead =
        destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
            new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(organizationId)
                .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));

    assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead);
    verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
  }

  @Test
  @DisplayName("revokeDestinationDefinitionFromWorkspace should correctly delete a workspace grant")
  void testRevokeDestinationDefinitionFromWorkspace() throws IOException {
    destinationDefinitionsHandler.revokeDestinationDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(workspaceId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE));
    verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), workspaceId,
        ScopeType.WORKSPACE);

    destinationDefinitionsHandler.revokeDestinationDefinition(
        new ActorDefinitionIdWithScope().actorDefinitionId(destinationDefinition.getDestinationDefinitionId()).scopeId(organizationId)
            .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION));
    verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(destinationDefinition.getDestinationDefinitionId(), organizationId,
        ScopeType.ORGANIZATION);
  }

  @SuppressWarnings("TypeName")
  @Nested
  @DisplayName("listLatest")
  class listLatest {

    @Test
    @DisplayName("should return the latest list")
    void testCorrect() {
      final ConnectorRegistryDestinationDefinition registryDestinationDefinition = new ConnectorRegistryDestinationDefinition()
          .withDestinationDefinitionId(UUID.randomUUID())
          .withName("some-destination")
          .withDocumentationUrl("https://airbyte.com")
          .withDockerRepository("dockerrepo")
          .withDockerImageTag("1.2.4")
          .withIcon("dest.svg")
          .withSpec(new ConnectorSpecification().withConnectionSpecification(
              Jsons.jsonNode(Map.of("key", "val"))))
          .withTombstone(false)
          .withProtocolVersion("0.2.2")
          .withSupportLevel(io.airbyte.config.SupportLevel.COMMUNITY)
          .withAbInternal(new AbInternal().withSl(100L))
          .withReleaseStage(io.airbyte.config.ReleaseStage.ALPHA)
          .withReleaseDate(TODAY_DATE_STRING)
          .withResourceRequirements(new ScopedResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")))
          .withLanguage("java");
      when(remoteDefinitionsProvider.getDestinationDefinitions()).thenReturn(Collections.singletonList(registryDestinationDefinition));

      final var destinationDefinitionReadList = destinationDefinitionsHandler.listLatestDestinationDefinitions().getDestinationDefinitions();
      assertEquals(1, destinationDefinitionReadList.size());

      final var destinationDefinitionRead = destinationDefinitionReadList.get(0);
      assertEquals(destinationDefinitionsHandler.buildDestinationDefinitionRead(
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
