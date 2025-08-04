/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.JobTypeResourceLimit
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.version.Version
import io.airbyte.config.AbInternal
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.AllowedHosts
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistryEntryMetrics
import io.airbyte.config.MapperConfig
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopeType
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SupportLevel
import io.airbyte.config.helpers.ConnectorRegistryConverters
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult
import io.airbyte.config.init.SupportStateUpdater
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.jooq.JSONB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.eq
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.time.LocalDate
import java.util.Date
import java.util.Map
import java.util.UUID
import java.util.function.Supplier

internal class DestinationDefinitionsHandlerTest {
  private lateinit var actorDefinitionService: ActorDefinitionService

  private lateinit var destinationDefinition: StandardDestinationDefinition
  private lateinit var destinationDefinitionWithOptionals: StandardDestinationDefinition

  private lateinit var destinationDefinitionVersion: ActorDefinitionVersion
  private lateinit var destinationDefinitionVersionWithOptionals: ActorDefinitionVersion

  private lateinit var destinationDefinitionsHandler: DestinationDefinitionsHandler
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var supportStateUpdater: SupportStateUpdater
  private lateinit var workspaceId: UUID
  private lateinit var organizationId: UUID
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator
  private lateinit var destinationService: DestinationService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var licenseEntitlementChecker: LicenseEntitlementChecker
  private var apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf<Mapper<out MapperConfig>>()))

  @BeforeEach
  fun setUp() {
    destinationService = Mockito.mock(DestinationService::class.java)
    workspaceService = Mockito.mock(WorkspaceService::class.java)
    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    uuidSupplier = Mockito.mock(Supplier::class.java) as Supplier<UUID>
    destinationDefinition = generateDestinationDefinition()
    destinationDefinitionWithOptionals = generateDestinationDefinitionWithOptionals()
    destinationDefinitionVersion = generateVersionFromDestinationDefinition(destinationDefinition)
    destinationDefinitionVersionWithOptionals = generateDestinationDefinitionVersionWithOptionals(destinationDefinitionWithOptionals)
    actorDefinitionHandlerHelper = Mockito.mock(ActorDefinitionHandlerHelper::class.java)
    remoteDefinitionsProvider = Mockito.mock(RemoteDefinitionsProvider::class.java)
    licenseEntitlementChecker = Mockito.mock(LicenseEntitlementChecker::class.java)
    destinationHandler = Mockito.mock(DestinationHandler::class.java)
    supportStateUpdater = Mockito.mock(SupportStateUpdater::class.java)
    workspaceId = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    featureFlagClient = Mockito.mock(TestClient::class.java)
    actorDefinitionVersionHelper = Mockito.mock(ActorDefinitionVersionHelper::class.java)
    airbyteCompatibleConnectorsValidator = Mockito.mock(AirbyteCompatibleConnectorsValidator::class.java)
    destinationDefinitionsHandler =
      DestinationDefinitionsHandler(
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
        apiPojoConverters,
      )

    Mockito.`when`(uuidSupplier.get()).thenReturn(UUID.randomUUID())
  }

  private fun generateDestinationDefinition(): StandardDestinationDefinition =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(UUID.randomUUID())
      .withDefaultVersionId(UUID.randomUUID())
      .withName("presto")
      .withIcon("http.svg")
      .withIconUrl(ICON_URL)
      .withTombstone(false)
      .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))

  private fun generateVersionFromDestinationDefinition(destinationDefinition: StandardDestinationDefinition): ActorDefinitionVersion {
    val spec =
      ConnectorSpecification()
        .withConnectionSpecification(jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("foo", "bar")))

    return ActorDefinitionVersion()
      .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
      .withDockerImageTag("12.3")
      .withDockerRepository("repo")
      .withDocumentationUrl("https://hulu.com")
      .withSpec(spec)
      .withProtocolVersion("0.2.2")
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.ALPHA)
      .withReleaseDate(todayDateString)
      .withAllowedHosts(AllowedHosts().withHosts(mutableListOf<String?>("host1", "host2")))
      .withLanguage("java")
      .withSupportsDataActivation(false)
  }

  private fun generateBreakingChangesFromDestinationDefinition(destDef: StandardDestinationDefinition): MutableList<ActorDefinitionBreakingChange> {
    val breakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(destDef.getDestinationDefinitionId())
        .withVersion(Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21")
    return mutableListOf(breakingChange)
  }

  private fun generateCustomVersionFromDestinationDefinition(destinationDefinition: StandardDestinationDefinition): ActorDefinitionVersion =
    generateVersionFromDestinationDefinition(destinationDefinition)
      .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
      .withReleaseDate(null)
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.CUSTOM)
      .withAllowedHosts(null)
      .withLanguage("manifest-only")

  private fun generateDestinationDefinitionWithOptionals(): StandardDestinationDefinition {
    val metrics =
      ConnectorRegistryEntryMetrics().withAdditionalProperty("all", JSONB.valueOf("{'all': {'usage': 'high'}}"))
    return generateDestinationDefinition().withMetrics(metrics)
  }

  private fun generateDestinationDefinitionVersionWithOptionals(destinationDefinition: StandardDestinationDefinition): ActorDefinitionVersion =
    generateVersionFromDestinationDefinition(destinationDefinition)
      .withCdkVersion("python:1.2.3")
      .withLastPublished(Date())

  @Test
  @DisplayName("listDestinationDefinition should return the right list")
  @Throws(IOException::class, URISyntaxException::class)
  fun testListDestinations() {
    Mockito
      .`when`(destinationService.listStandardDestinationDefinitions(false))
      .thenReturn(listOf(destinationDefinition, destinationDefinitionWithOptionals))
    Mockito
      .`when`(
        actorDefinitionService.getActorDefinitionVersions(
          listOf<UUID?>(
            destinationDefinition.getDefaultVersionId(),
            destinationDefinitionWithOptionals.getDefaultVersionId(),
          ),
        ),
      ).thenReturn(
        listOf(
          destinationDefinitionVersion,
          destinationDefinitionVersionWithOptionals,
        ),
      )

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val expectedDestinationDefinitionReadWithOpts =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinitionWithOptionals.getDestinationDefinitionId())
        .name(destinationDefinitionWithOptionals.getName())
        .dockerRepository(destinationDefinitionVersionWithOptionals.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersionWithOptionals.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersionWithOptionals.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersionWithOptionals.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel.fromValue(
            destinationDefinitionVersionWithOptionals.getSupportLevel().value(),
          ),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage.fromValue(
            destinationDefinitionVersionWithOptionals.getReleaseStage().value(),
          ),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersionWithOptionals.getReleaseDate()))
        .cdkVersion(destinationDefinitionVersionWithOptionals.getCdkVersion())
        .lastPublished(apiPojoConverters.toOffsetDateTime(destinationDefinitionVersionWithOptionals.getLastPublished()))
        .metrics(destinationDefinitionWithOptionals.getMetrics())
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinitionWithOptionals.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersionWithOptionals.getLanguage())
        .supportsDataActivation(destinationDefinitionVersionWithOptionals.getSupportsDataActivation())

    val actualDestinationDefinitionReadList = destinationDefinitionsHandler.listDestinationDefinitions()

    Assertions.assertEquals(
      listOf<DestinationDefinitionRead?>(expectedDestinationDefinitionRead1, expectedDestinationDefinitionReadWithOpts),
      actualDestinationDefinitionReadList.getDestinationDefinitions(),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list")
  @Throws(IOException::class, URISyntaxException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testListDestinationDefinitionsForWorkspace() {
    Mockito
      .`when`(
        featureFlagClient.boolVariation(
          eq(HideActorDefinitionFromList),
          anyOrNull(),
        ),
      ).thenReturn(false)
    val workspace = Mockito.mock(StandardWorkspace::class.java)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)
    Mockito.`when`(workspace.getOrganizationId()).thenReturn(UUID.randomUUID())
    Mockito
      .`when`(destinationService.listPublicDestinationDefinitions(false))
      .thenReturn(listOf<StandardDestinationDefinition>(destinationDefinition))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            destinationDefinition,
          ),
          workspaceId,
        ),
      ).thenReturn(Map.of<UUID, ActorDefinitionVersion?>(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion))
    Mockito
      .`when`(
        licenseEntitlementChecker.checkEntitlements(
          anyOrNull(),
          eq(Entitlement.DESTINATION_CONNECTOR),
          eq(listOf(destinationDefinition.getDestinationDefinitionId())),
        ),
      ).thenReturn(mapOf(destinationDefinition.getDestinationDefinitionId() to true))

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    Assertions.assertEquals(
      listOf<DestinationDefinitionRead?>(expectedDestinationDefinitionRead1),
      actualDestinationDefinitionReadList.getDestinationDefinitions(),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out unentitled connectors")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListDestinationDefinitionsForWorkspaceWithUnentitledConnectors() {
    val unentitledDestinationDefinition = generateDestinationDefinition()

    Mockito
      .`when`(
        featureFlagClient.boolVariation(
          eq(HideActorDefinitionFromList),
          anyOrNull(),
        ),
      ).thenReturn(false)
    val workspace = Mockito.mock(StandardWorkspace::class.java)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)
    Mockito.`when`(workspace.getOrganizationId()).thenReturn(UUID.randomUUID())

    Mockito
      .`when`(
        licenseEntitlementChecker.checkEntitlements(
          anyOrNull(),
          eq(Entitlement.DESTINATION_CONNECTOR),
          eq(
            listOf(
              destinationDefinition.getDestinationDefinitionId(),
              unentitledDestinationDefinition.getDestinationDefinitionId(),
            ),
          ),
        ),
      ).thenReturn(
        mapOf(
          destinationDefinition.getDestinationDefinitionId() to true,
          unentitledDestinationDefinition.getDestinationDefinitionId() to false,
        ),
      )

    Mockito
      .`when`(destinationService.listPublicDestinationDefinitions(false))
      .thenReturn(listOf<StandardDestinationDefinition>(destinationDefinition, unentitledDestinationDefinition))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            destinationDefinition,
          ),
          workspaceId,
        ),
      ).thenReturn(Map.of<UUID, ActorDefinitionVersion?>(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion))

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    val expectedIds =
      listOf<UUID?>(destinationDefinition.getDestinationDefinitionId())

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.getDestinationDefinitions().size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .getDestinationDefinitions()
          .stream()
          .map<UUID?> { obj: DestinationDefinitionRead? -> obj!!.getDestinationDefinitionId() }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListDestinationDefinitionsForWorkspaceWithHiddenConnectors() {
    val hiddenDestinationDefinition = generateDestinationDefinition()

    Mockito
      .`when`(
        featureFlagClient.boolVariation(
          eq(HideActorDefinitionFromList),
          anyOrNull(),
        ),
      ).thenReturn(false)
    Mockito
      .`when`<Boolean?>(
        featureFlagClient.boolVariation(
          HideActorDefinitionFromList,
          Multi(listOf(DestinationDefinition(hiddenDestinationDefinition.getDestinationDefinitionId()), Workspace(workspaceId))),
        ),
      ).thenReturn(true)

    val workspace = Mockito.mock(StandardWorkspace::class.java)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)
    Mockito.`when`(workspace.getOrganizationId()).thenReturn(UUID.randomUUID())
    Mockito
      .`when`(
        licenseEntitlementChecker.checkEntitlements(
          anyOrNull(),
          eq(Entitlement.DESTINATION_CONNECTOR),
          eq(
            listOf(
              destinationDefinition.getDestinationDefinitionId(),
              hiddenDestinationDefinition.getDestinationDefinitionId(),
            ),
          ),
        ),
      ).thenReturn(
        mapOf(
          destinationDefinition.getDestinationDefinitionId() to true,
          hiddenDestinationDefinition.getDestinationDefinitionId() to true,
        ),
      )

    Mockito
      .`when`(destinationService.listPublicDestinationDefinitions(false))
      .thenReturn(listOf<StandardDestinationDefinition>(destinationDefinition, hiddenDestinationDefinition))
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            destinationDefinition,
          ),
          workspaceId,
        ),
      ).thenReturn(Map.of<UUID, ActorDefinitionVersion?>(destinationDefinitionVersion.getActorDefinitionId(), destinationDefinitionVersion))

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    val expectedIds =
      listOf<UUID?>(destinationDefinition.getDestinationDefinitionId())

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.getDestinationDefinitions().size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .getDestinationDefinitions()
          .stream()
          .map<UUID?> { obj: DestinationDefinitionRead? -> obj!!.getDestinationDefinitionId() }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return the right list")
  @Throws(IOException::class, URISyntaxException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testListDestinationDefinitionsUsedByWorkspace() {
    val usedDestinationDefinition = generateDestinationDefinition()
    val usedDestinationDefinitionVersion = generateVersionFromDestinationDefinition(usedDestinationDefinition)

    Mockito
      .`when`(
        destinationService.listDestinationDefinitionsForWorkspace(
          workspaceId,
          false,
        ),
      ).thenReturn(
        listOf<StandardDestinationDefinition>(usedDestinationDefinition),
      )
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            usedDestinationDefinition,
          ),
          workspaceId,
        ),
      ).thenReturn(
        Map.of<UUID, ActorDefinitionVersion?>(
          usedDestinationDefinitionVersion.getActorDefinitionId(),
          usedDestinationDefinitionVersion,
        ),
      )

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(true))

    val expectedIds =
      listOf<UUID?>(usedDestinationDefinition.getDestinationDefinitionId())

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.getDestinationDefinitions().size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .getDestinationDefinitions()
          .stream()
          .map<UUID?> { obj: DestinationDefinitionRead? -> obj!!.getDestinationDefinitionId() }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return all definitions when filterByUsed is false")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListDestinationDefinitionsUsedByWorkspaceWithFilterByUsedFalse() {
    val destinationDefinition2 = generateDestinationDefinition()
    val destinationDefinitionVersion2 = generateVersionFromDestinationDefinition(destinationDefinition2)

    Mockito
      .`when`(
        featureFlagClient.boolVariation(
          eq(HideActorDefinitionFromList),
          anyOrNull(),
        ),
      ).thenReturn(false)
    Mockito
      .`when`(destinationService.listPublicDestinationDefinitions(false))
      .thenReturn(listOf<StandardDestinationDefinition>(destinationDefinition))
    Mockito
      .`when`(destinationService.listGrantedDestinationDefinitions(workspaceId, false))
      .thenReturn(
        listOf<StandardDestinationDefinition>(destinationDefinition2),
      )
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            destinationDefinition,
            destinationDefinition2,
          ),
          workspaceId,
        ),
      ).thenReturn(
        Map.of<UUID, ActorDefinitionVersion?>(
          destinationDefinitionVersion.getActorDefinitionId(),
          destinationDefinitionVersion,
          destinationDefinitionVersion2.getActorDefinitionId(),
          destinationDefinitionVersion2,
        ),
      )
    val workspace = Mockito.mock(StandardWorkspace::class.java)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)
    Mockito.`when`(workspace.getOrganizationId()).thenReturn(UUID.randomUUID())
    Mockito
      .`when`(
        licenseEntitlementChecker.checkEntitlements(
          anyOrNull(),
          eq(Entitlement.DESTINATION_CONNECTOR),
          eq(listOf(destinationDefinition.getDestinationDefinitionId())),
        ),
      ).thenReturn(mapOf(destinationDefinition.getDestinationDefinitionId() to true))

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(false))

    val expectedIds = listOf<UUID?>(destinationDefinition.getDestinationDefinitionId(), destinationDefinition2.getDestinationDefinitionId())
    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.getDestinationDefinitions().size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .getDestinationDefinitions()
          .stream()
          .map<UUID?> { obj: DestinationDefinitionRead? -> obj!!.getDestinationDefinitionId() }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return only used definitions when filterByUsed is true")
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testListDestinationDefinitionsUsedByWorkspaceWithFilterByUsedTrue() {
    val usedDestinationDefinition = generateDestinationDefinition()
    val usedDestinationDefinitionVersion = generateVersionFromDestinationDefinition(usedDestinationDefinition)

    Mockito
      .`when`(
        destinationService.listDestinationDefinitionsForWorkspace(
          workspaceId,
          false,
        ),
      ).thenReturn(
        listOf<StandardDestinationDefinition>(usedDestinationDefinition),
      )
    Mockito
      .`when`(
        actorDefinitionVersionHelper.getDestinationVersions(
          listOf(
            usedDestinationDefinition,
          ),
          workspaceId,
        ),
      ).thenReturn(
        Map.of<UUID, ActorDefinitionVersion?>(
          usedDestinationDefinitionVersion.getActorDefinitionId(),
          usedDestinationDefinitionVersion,
        ),
      )

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(true))

    val expectedIds = listOf<UUID?>(usedDestinationDefinition.getDestinationDefinitionId())
    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.getDestinationDefinitions().size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .getDestinationDefinitions()
          .stream()
          .map<UUID?> { obj: DestinationDefinitionRead? -> obj!!.getDestinationDefinitionId() }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listPrivateDestinationDefinitions should return the right list")
  @Throws(IOException::class, URISyntaxException::class)
  fun testListPrivateDestinationDefinitions() {
    Mockito
      .`when`(
        destinationService.listGrantableDestinationDefinitions(
          workspaceId,
          false,
        ),
      ).thenReturn(
        listOf<MutableMap.MutableEntry<StandardDestinationDefinition, Boolean>>(
          Map.entry<StandardDestinationDefinition, Boolean>(
            destinationDefinition,
            false,
          ),
        ),
      )
    Mockito
      .`when`(
        actorDefinitionService.getActorDefinitionVersions(
          listOf<UUID?>(destinationDefinition.getDefaultVersionId()),
        ),
      ).thenReturn(listOf<ActorDefinitionVersion>(destinationDefinitionVersion))

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val expectedDestinationDefinitionOptInRead1 =
      PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead1).granted(false)

    val actualDestinationDefinitionOptInReadList =
      destinationDefinitionsHandler.listPrivateDestinationDefinitions(
        WorkspaceIdRequestBody().workspaceId(workspaceId),
      )

    Assertions.assertEquals(
      listOf<PrivateDestinationDefinitionRead?>(expectedDestinationDefinitionOptInRead1),
      actualDestinationDefinitionOptInReadList.getDestinationDefinitions(),
    )
  }

  @Test
  @DisplayName("getDestinationDefinition should return the right destination")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGetDestination() {
    Mockito
      .`when`(
        destinationService.getStandardDestinationDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          true,
        ),
      ).thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val actualDestinationDefinitionRead =
      destinationDefinitionsHandler.getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true)

    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead)
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should throw an exception for a missing grant")
  @Throws(IOException::class)
  fun testGetDefinitionWithoutGrantForWorkspace() {
    Mockito
      .`when`(workspaceService.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId))
      .thenReturn(false)

    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId)

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId) }
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should throw an exception for a missing grant")
  @Throws(IOException::class)
  fun testGetDefinitionWithoutGrantForScope() {
    Mockito
      .`when`(
        actorDefinitionService.scopeCanUseDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          workspaceId,
          ScopeType.WORKSPACE.value(),
        ),
      ).thenReturn(false)
    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForWorkspace) }

    Mockito
      .`when`(
        actorDefinitionService.scopeCanUseDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          organizationId,
          ScopeType.ORGANIZATION.value(),
        ),
      ).thenReturn(false)
    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForOrganization) }
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should return the destination definition if the grant exists")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGetDefinitionWithGrantForWorkspace() {
    Mockito
      .`when`(workspaceService.workspaceCanUseDefinition(destinationDefinition.getDestinationDefinitionId(), workspaceId))
      .thenReturn(true)
    Mockito
      .`when`(
        destinationService.getStandardDestinationDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          true,
        ),
      ).thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .workspaceId(workspaceId)

    val actualDestinationDefinitionRead =
      destinationDefinitionsHandler
        .getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId)

    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead)
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should return the destination definition if the grant exists")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGetDefinitionWithGrantForScope() {
    Mockito
      .`when`(
        actorDefinitionService.scopeCanUseDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          workspaceId,
          ScopeType.WORKSPACE.value(),
        ),
      ).thenReturn(true)
    Mockito
      .`when`(
        actorDefinitionService.scopeCanUseDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          organizationId,
          ScopeType.ORGANIZATION.value(),
        ),
      ).thenReturn(true)
    Mockito
      .`when`(
        destinationService.getStandardDestinationDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          true,
        ),
      ).thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)

    val actualDestinationDefinitionReadForWorkspace =
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace,
      )
    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForWorkspace)

    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)

    val actualDestinationDefinitionReadForOrganization =
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization,
      )
    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForOrganization)
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition")
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomDestinationDefinition() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition)

    Mockito.`when`(uuidSupplier.get()).thenReturn(newDestinationDefinition.getDestinationDefinitionId())

    val create =
      DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreate =
      CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId)

    Mockito
      .`when`(
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          create.getDockerRepository(),
          create.getDockerImageTag(),
          create.getDocumentationUrl(),
          customCreate.getWorkspaceId(),
        ),
      ).thenReturn(destinationDefinitionVersion)

    val expectedRead =
      DestinationDefinitionRead()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(newDestinationDefinition.getDestinationDefinitionId())
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(io.airbyte.api.model.generated.ReleaseStage.CUSTOM)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val actualRead = destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate)

    Assertions.assertEquals(expectedRead, actualRead)
    Mockito.verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.getDockerRepository(),
      create.getDockerImageTag(),
      create.getDocumentationUrl(),
      customCreate.getWorkspaceId(),
    )
    Mockito.verify(destinationService).writeCustomConnectorMetadata(
      newDestinationDefinition
        .withCustom(true)
        .withDefaultVersionId(null)
        .withIconUrl(null),
      destinationDefinitionVersion,
      workspaceId,
      ScopeType.WORKSPACE,
    )

    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition for a workspace and organization using scopes")
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomDestinationDefinitionUsingScopes() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition)

    Mockito.`when`(uuidSupplier.get()).thenReturn(newDestinationDefinition.getDestinationDefinitionId())

    val create =
      DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreateForWorkspace =
      CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .workspaceId(null) // scopeType and scopeId should be sufficient to resolve to the expected workspaceId

    Mockito
      .`when`(
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          create.getDockerRepository(),
          create.getDockerImageTag(),
          create.getDocumentationUrl(),
          workspaceId,
        ),
      ).thenReturn(destinationDefinitionVersion)

    val expectedRead =
      DestinationDefinitionRead()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .destinationDefinitionId(newDestinationDefinition.getDestinationDefinitionId())
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(io.airbyte.api.model.generated.ReleaseStage.CUSTOM)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val actualRead =
      destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForWorkspace)

    Assertions.assertEquals(expectedRead, actualRead)
    Mockito.verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.getDockerRepository(),
      create.getDockerImageTag(),
      create.getDocumentationUrl(),
      workspaceId,
    )
    Mockito.verify(destinationService).writeCustomConnectorMetadata(
      newDestinationDefinition
        .withCustom(true)
        .withDefaultVersionId(null)
        .withIconUrl(null),
      destinationDefinitionVersion,
      workspaceId,
      ScopeType.WORKSPACE,
    )

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
    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "createCustomDestinationDefinition should not create a destinationDefinition " +
        "if defaultDefinitionVersionFromCreate throws unsupported protocol version error"
    ),
  )
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomDestinationDefinitionShouldCheckProtocolVersion() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateVersionFromDestinationDefinition(newDestinationDefinition)

    val create =
      DestinationDefinitionCreate()
        .name(newDestinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(newDestinationDefinition.getIcon())
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newDestinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreate =
      CustomDestinationDefinitionCreate()
        .destinationDefinition(create)
        .workspaceId(workspaceId)

    Mockito
      .`when`(
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
          create.getDockerRepository(),
          create.getDockerImageTag(),
          create.getDocumentationUrl(),
          customCreate.getWorkspaceId(),
        ),
      ).thenThrow(UnsupportedProtocolVersionException::class.java)
    Assertions.assertThrows(
      UnsupportedProtocolVersionException::class.java,
    ) { destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate) }

    Mockito.verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.getDockerRepository(),
      create.getDockerImageTag(),
      create.getDocumentationUrl(),
      customCreate.getWorkspaceId(),
    )
    Mockito.verify(destinationService, Mockito.never()).writeCustomConnectorMetadata(
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
    )

    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("updateDestinationDefinition should correctly update a destinationDefinition")
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class, URISyntaxException::class)
  fun testUpdateDestination() {
    Mockito
      .`when`(
        airbyteCompatibleConnectorsValidator.validate(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))

    val newDockerImageTag = "averydifferenttag"
    val updatedDestination =
      clone(destinationDefinition).withDefaultVersionId(null)
    val updatedDestinationDefVersion =
      generateVersionFromDestinationDefinition(updatedDestination)
        .withDockerImageTag(newDockerImageTag)
        .withVersionId(UUID.randomUUID())

    val persistedUpdatedDestination =
      clone<StandardDestinationDefinition>(updatedDestination).withDefaultVersionId(updatedDestinationDefVersion.getVersionId())

    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition) // Call at the beginning of the method
      .thenReturn(persistedUpdatedDestination) // Call after we've persisted

    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    Mockito
      .`when`(
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          destinationDefinitionVersion,
          ActorType.DESTINATION,
          newDockerImageTag,
          destinationDefinition.getCustom(),
          workspaceId,
        ),
      ).thenReturn(updatedDestinationDefVersion)

    val breakingChanges: MutableList<ActorDefinitionBreakingChange> = generateBreakingChangesFromDestinationDefinition(updatedDestination)
    Mockito
      .`when`(
        actorDefinitionHandlerHelper.getBreakingChanges(
          updatedDestinationDefVersion,
          ActorType.DESTINATION,
        ),
      ).thenReturn(breakingChanges)

    val destinationRead =
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate()
          .destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
          .dockerImageTag(newDockerImageTag)
          .workspaceId(workspaceId),
      )

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(newDockerImageTag)
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    Assertions.assertEquals(expectedDestinationDefinitionRead, destinationRead)
    Mockito.verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(
      destinationDefinitionVersion,
      ActorType.DESTINATION,
      newDockerImageTag,
      destinationDefinition.getCustom(),
      workspaceId,
    )
    Mockito
      .verify(actorDefinitionHandlerHelper)
      .getBreakingChanges(updatedDestinationDefVersion, ActorType.DESTINATION)
    Mockito
      .verify(destinationService)
      .writeConnectorMetadata(updatedDestination, updatedDestinationDefVersion, breakingChanges)
    Mockito.verify(supportStateUpdater).updateSupportStatesForDestinationDefinition(persistedUpdatedDestination)
    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper, supportStateUpdater)
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  fun testBuildDestinationDefinitionUpdateNameNonCustom() {
    val existingDestinationDefinition = destinationDefinition

    val destinationDefinitionUpdate =
      DestinationDefinitionUpdate()
        .destinationDefinitionId(existingDestinationDefinition.getDestinationDefinitionId())
        .name("Some name that gets ignored")

    val newDestinationDefinition =
      destinationDefinitionsHandler.buildDestinationDefinitionUpdate(existingDestinationDefinition, destinationDefinitionUpdate)

    Assertions.assertEquals(newDestinationDefinition.getName(), existingDestinationDefinition.getName())
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  fun testBuildDestinationDefinitionUpdateNameCustom() {
    val newName = "My new connector name"
    val existingCustomDestinationDefinition = generateDestinationDefinition().withCustom(true)

    val destinationDefinitionUpdate =
      DestinationDefinitionUpdate()
        .destinationDefinitionId(existingCustomDestinationDefinition.getDestinationDefinitionId())
        .name(newName)

    val newDestinationDefinition =
      destinationDefinitionsHandler.buildDestinationDefinitionUpdate(
        existingCustomDestinationDefinition,
        destinationDefinitionUpdate,
      )

    Assertions.assertEquals(newDestinationDefinition.getName(), newName)
  }

  @Test
  @DisplayName(
    (
      "updateDestinationDefinition should not update a destinationDefinition " +
        "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error"
    ),
  )
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testOutOfProtocolRangeUpdateDestination() {
    Mockito
      .`when`(
        airbyteCompatibleConnectorsValidator.validate(
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))
    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition)
    Mockito
      .`when`(
        destinationService.getStandardDestinationDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          true,
        ),
      ).thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)
    val currentDestination =
      destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true)
    val currentTag = currentDestination.getDockerImageTag()
    val newDockerImageTag = "averydifferenttagforprotocolversion"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    Mockito
      .`when`(
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          destinationDefinitionVersion,
          ActorType.DESTINATION,
          newDockerImageTag,
          destinationDefinition.getCustom(),
          workspaceId,
        ),
      ).thenThrow(UnsupportedProtocolVersionException::class.java)

    Assertions.assertThrows(UnsupportedProtocolVersionException::class.java) {
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate()
          .destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
          .dockerImageTag(newDockerImageTag)
          .workspaceId(workspaceId),
      )
    }

    Mockito.verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(
      destinationDefinitionVersion,
      ActorType.DESTINATION,
      newDockerImageTag,
      destinationDefinition.getCustom(),
      workspaceId,
    )
    Mockito.verify(destinationService, Mockito.never()).writeConnectorMetadata(
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
    )

    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "updateDestinationDefinition should not update a destinationDefinition " +
        "if Airbyte version is unsupported"
    ),
  )
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testUnsupportedAirbyteVersionUpdateDestination() {
    Mockito
      .`when`(
        airbyteCompatibleConnectorsValidator.validate(
          anyOrNull(),
          eq("12.4.0"),
        ),
      ).thenReturn(ConnectorPlatformCompatibilityValidationResult(false, ""))
    Mockito
      .`when`(
        destinationService.getStandardDestinationDefinition(
          destinationDefinition.getDestinationDefinitionId(),
          true,
        ),
      ).thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)
    val currentDestination =
      destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.getDestinationDefinitionId(), true)
    val currentTag = currentDestination.getDockerImageTag()
    val newDockerImageTag = "12.4.0"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    Assertions.assertThrows(BadRequestProblem::class.java) {
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate()
          .destinationDefinitionId(this.destinationDefinition.getDestinationDefinitionId())
          .dockerImageTag(newDockerImageTag),
      )
    }
    Mockito.verify(destinationService, Mockito.never()).writeConnectorMetadata(
      anyOrNull(),
      anyOrNull(),
      anyOrNull(),
    )
    Mockito.verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("deleteDestinationDefinition should correctly delete a sourceDefinition")
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDeleteDestinationDefinition() {
    val updatedDestinationDefinition = clone<StandardDestinationDefinition>(this.destinationDefinition).withTombstone(true)
    val newDestinationDefinition = DestinationRead()

    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition)
    Mockito
      .`when`(destinationHandler.listDestinationsForDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(DestinationReadList().destinations(mutableListOf<@Valid DestinationRead?>(newDestinationDefinition)))

    Assertions.assertFalse(destinationDefinition.getTombstone())

    destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinition.getDestinationDefinitionId())

    Mockito.verify(destinationHandler).deleteDestination(newDestinationDefinition)
    Mockito.verify(destinationService).updateStandardDestinationDefinition(updatedDestinationDefinition)
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspace should correctly create a workspace grant")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGrantDestinationDefinitionToWorkspace() {
    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val expectedPrivateDestinationDefinitionRead =
      PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true)

    val actualPrivateDestinationDefinitionRead =
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope()
          .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
          .scopeId(workspaceId)
          .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE),
      )

    Assertions.assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead)
    Mockito.verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(
      destinationDefinition.getDestinationDefinitionId(),
      workspaceId,
      ScopeType.WORKSPACE,
    )
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspaceOrOrganization should correctly create an organization grant")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGrantDestinationDefinitionToOrganization() {
    Mockito
      .`when`(destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition)
    Mockito
      .`when`(actorDefinitionService.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId()))
      .thenReturn(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead()
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .name(destinationDefinition.getName())
        .dockerRepository(destinationDefinitionVersion.getDockerRepository())
        .dockerImageTag(destinationDefinitionVersion.getDockerImageTag())
        .documentationUrl(URI(destinationDefinitionVersion.getDocumentationUrl()))
        .icon(ICON_URL)
        .protocolVersion(destinationDefinitionVersion.getProtocolVersion())
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(destinationDefinitionVersion.getSupportLevel().value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(destinationDefinitionVersion.getReleaseStage().value()),
        ).releaseDate(LocalDate.parse(destinationDefinitionVersion.getReleaseDate()))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(destinationDefinition.getResourceRequirements().getDefault().getCpuRequest()),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(destinationDefinitionVersion.getLanguage())
        .supportsDataActivation(destinationDefinitionVersion.getSupportsDataActivation())

    val expectedPrivateDestinationDefinitionRead =
      PrivateDestinationDefinitionRead().destinationDefinition(expectedDestinationDefinitionRead).granted(true)

    val actualPrivateDestinationDefinitionRead =
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope()
          .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
          .scopeId(organizationId)
          .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION),
      )

    Assertions.assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead)
    Mockito.verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(
      destinationDefinition.getDestinationDefinitionId(),
      organizationId,
      ScopeType.ORGANIZATION,
    )
  }

  @Test
  @DisplayName("revokeDestinationDefinitionFromWorkspace should correctly delete a workspace grant")
  @Throws(IOException::class)
  fun testRevokeDestinationDefinitionFromWorkspace() {
    destinationDefinitionsHandler.revokeDestinationDefinition(
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE),
    )
    Mockito.verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(
      destinationDefinition.getDestinationDefinitionId(),
      workspaceId,
      ScopeType.WORKSPACE,
    )

    destinationDefinitionsHandler.revokeDestinationDefinition(
      ActorDefinitionIdWithScope()
        .actorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION),
    )
    Mockito.verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(
      destinationDefinition.getDestinationDefinitionId(),
      organizationId,
      ScopeType.ORGANIZATION,
    )
  }

  @Nested
  @DisplayName("listLatest")
  internal inner class ListLatest {
    @Test
    @DisplayName("should return the latest list")
    fun testCorrect() {
      val registryDestinationDefinition =
        ConnectorRegistryDestinationDefinition()
          .withDestinationDefinitionId(UUID.randomUUID())
          .withName("some-destination")
          .withDocumentationUrl("https://airbyte.com")
          .withDockerRepository("dockerrepo")
          .withDockerImageTag("1.2.4")
          .withIcon("dest.svg")
          .withSpec(
            ConnectorSpecification().withConnectionSpecification(
              jsonNode<MutableMap<String?, String?>?>(Map.of<String?, String?>("key", "val")),
            ),
          ).withTombstone(false)
          .withProtocolVersion("0.2.2")
          .withSupportLevel(SupportLevel.COMMUNITY)
          .withAbInternal(AbInternal().withSl(100L))
          .withReleaseStage(ReleaseStage.ALPHA)
          .withReleaseDate(todayDateString)
          .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))
          .withLanguage("java")
      Mockito.`when`(remoteDefinitionsProvider.getDestinationDefinitions()).thenReturn(
        mutableListOf<ConnectorRegistryDestinationDefinition>(registryDestinationDefinition!!),
      )

      val destinationDefinitionReadList = destinationDefinitionsHandler.listLatestDestinationDefinitions().getDestinationDefinitions()
      Assertions.assertEquals(1, destinationDefinitionReadList.size)

      val destinationDefinitionRead = destinationDefinitionReadList.get(0)
      Assertions.assertEquals(
        destinationDefinitionsHandler.buildDestinationDefinitionRead(
          ConnectorRegistryConverters.toStandardDestinationDefinition(registryDestinationDefinition),
          ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDefinition),
        ),
        destinationDefinitionRead,
      )
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    fun testHttpTimeout() {
      Mockito.`when`(remoteDefinitionsProvider.getDestinationDefinitions()).thenThrow(
        RuntimeException(),
      )
      Assertions.assertEquals(0, destinationDefinitionsHandler.listLatestDestinationDefinitions().getDestinationDefinitions().size)
    }
  }

  companion object {
    private val todayDateString = LocalDate.now().toString()
    private const val DEFAULT_PROTOCOL_VERSION = "0.2.0"
    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-presto/latest/icon.svg"
  }
}
