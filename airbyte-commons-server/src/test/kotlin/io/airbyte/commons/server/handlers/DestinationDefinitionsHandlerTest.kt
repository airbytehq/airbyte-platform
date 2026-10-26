/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomDestinationDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionUpdate
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionRead
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.enums.convertTo
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
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.jooq.JSONB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.LocalDate
import java.util.Date
import java.util.UUID
import java.util.function.Supplier
import io.airbyte.api.server.generated.models.ReleaseStage as KotlinReleaseStage
import io.airbyte.api.server.generated.models.ResourceRequirements as KotlinResourceRequirements
import io.airbyte.api.server.generated.models.ScopeType as KotlinScopeType
import io.airbyte.api.server.generated.models.ScopedResourceRequirements as KotlinScopedResourceRequirements
import io.airbyte.api.server.generated.models.SupportLevel as KotlinSupportLevel

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
  private var apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  @BeforeEach
  fun setUp() {
    destinationService = mockk<DestinationService>()
    workspaceService = mockk<WorkspaceService>()
    actorDefinitionService = mockk<ActorDefinitionService>()
    uuidSupplier = mockk<Supplier<UUID>>()
    destinationDefinition = generateDestinationDefinition()
    destinationDefinitionWithOptionals = generateDestinationDefinitionWithOptionals()
    destinationDefinitionVersion = generateVersionFromDestinationDefinition(destinationDefinition)
    destinationDefinitionVersionWithOptionals = generateDestinationDefinitionVersionWithOptionals(destinationDefinitionWithOptionals)
    actorDefinitionHandlerHelper = mockk<ActorDefinitionHandlerHelper>()
    remoteDefinitionsProvider = mockk<RemoteDefinitionsProvider>()
    licenseEntitlementChecker = mockk<LicenseEntitlementChecker>()
    destinationHandler = mockk<DestinationHandler>()
    supportStateUpdater = mockk<SupportStateUpdater>()
    workspaceId = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    featureFlagClient = mockk<TestClient>()
    actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    airbyteCompatibleConnectorsValidator = mockk<AirbyteCompatibleConnectorsValidator>()
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

    every { uuidSupplier.get() } returns UUID.randomUUID()
    every { actorDefinitionService.writeActorDefinitionWorkspaceGrant(any(), any(), any()) } just Runs
    every { actorDefinitionService.deleteActorDefinitionWorkspaceGrant(any(), any(), any()) } just Runs
    every { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) } just Runs
    every { destinationService.writeCustomConnectorMetadata(any(), any(), any(), any()) } just Runs
    every { destinationService.writeConnectorMetadata(any(), any(), any()) } just Runs
    every { destinationService.updateStandardDestinationDefinition(any()) } just Runs
    every { destinationHandler.deleteDestination(any<DestinationRead>()) } just Runs
    every { supportStateUpdater.updateSupportStatesForDestinationDefinition(any()) } just Runs
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
        .withConnectionSpecification(jsonNode(mapOf("foo" to "bar")))

    return ActorDefinitionVersion()
      .withActorDefinitionId(destinationDefinition.destinationDefinitionId)
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
        .withActorDefinitionId(destDef.destinationDefinitionId)
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
  fun testListDestinations() {
    every { destinationService.listStandardDestinationDefinitions(false) } returns listOf(destinationDefinition, destinationDefinitionWithOptionals)
    every {
      actorDefinitionService.getActorDefinitionVersions(
        listOf<UUID?>(
          destinationDefinition.defaultVersionId,
          destinationDefinitionWithOptionals.defaultVersionId,
        ),
      )
    } returns
      listOf(
        destinationDefinitionVersion,
        destinationDefinitionVersionWithOptionals,
      )

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val expectedDestinationDefinitionReadWithOpts =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinitionWithOptionals.destinationDefinitionId,
        name = destinationDefinitionWithOptionals.name,
        dockerRepository = destinationDefinitionVersionWithOptionals.dockerRepository,
        dockerImageTag = destinationDefinitionVersionWithOptionals.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersionWithOptionals.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersionWithOptionals.protocolVersion,
        supportLevel = destinationDefinitionVersionWithOptionals.supportLevel.convertTo<KotlinSupportLevel>(),
        releaseStage = destinationDefinitionVersionWithOptionals.releaseStage.convertTo<KotlinReleaseStage>(),
        releaseDate = LocalDate.parse(destinationDefinitionVersionWithOptionals.releaseDate),
        cdkVersion = destinationDefinitionVersionWithOptionals.cdkVersion,
        lastPublished = apiPojoConverters.toOffsetDateTime(destinationDefinitionVersionWithOptionals.lastPublished),
        metrics = destinationDefinitionWithOptionals.metrics,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinitionWithOptionals.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersionWithOptionals.language,
        supportsDataActivation = destinationDefinitionVersionWithOptionals.supportsDataActivation,
      )

    val actualDestinationDefinitionReadList = destinationDefinitionsHandler.listDestinationDefinitions()

    Assertions.assertEquals(
      listOf<DestinationDefinitionRead?>(expectedDestinationDefinitionRead1, expectedDestinationDefinitionReadWithOpts),
      actualDestinationDefinitionReadList.destinationDefinitions,
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list")
  fun testListDestinationDefinitionsForWorkspace() {
    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    val workspace = mockk<StandardWorkspace>()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace
    every { workspace.organizationId } returns UUID.randomUUID()
    every { destinationService.listPublicDestinationDefinitions(false) } returns listOf(destinationDefinition)
    every { destinationService.listGrantedDestinationDefinitions(workspaceId, false) } returns emptyList()
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          destinationDefinition,
        ),
        workspaceId,
      )
    } returns mapOf(destinationDefinitionVersion.actorDefinitionId to destinationDefinitionVersion)
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        listOf(destinationDefinition.destinationDefinitionId),
      )
    } returns mapOf(destinationDefinition.destinationDefinitionId to true)

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    Assertions.assertEquals(
      listOf<DestinationDefinitionRead?>(expectedDestinationDefinitionRead1),
      actualDestinationDefinitionReadList.destinationDefinitions,
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out unentitled connectors")
  fun testListDestinationDefinitionsForWorkspaceWithUnentitledConnectors() {
    val unentitledDestinationDefinition = generateDestinationDefinition()

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    val workspace = mockk<StandardWorkspace>()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace
    every { workspace.organizationId } returns UUID.randomUUID()

    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        listOf(
          destinationDefinition.destinationDefinitionId,
          unentitledDestinationDefinition.destinationDefinitionId,
        ),
      )
    } returns
      mapOf(
        destinationDefinition.destinationDefinitionId to true,
        unentitledDestinationDefinition.destinationDefinitionId to false,
      )

    every { destinationService.listPublicDestinationDefinitions(false) } returns listOf(destinationDefinition, unentitledDestinationDefinition)
    every { destinationService.listGrantedDestinationDefinitions(workspaceId, false) } returns emptyList()
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          destinationDefinition,
        ),
        workspaceId,
      )
    } returns mapOf(destinationDefinitionVersion.actorDefinitionId to destinationDefinitionVersion)

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    val expectedIds =
      listOf<UUID?>(destinationDefinition.destinationDefinitionId)

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.destinationDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .destinationDefinitions
          .map { obj: DestinationDefinitionRead? -> obj!!.destinationDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  fun testListDestinationDefinitionsForWorkspaceWithHiddenConnectors() {
    val hiddenDestinationDefinition = generateDestinationDefinition()

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        Multi(listOf(DestinationDefinition(hiddenDestinationDefinition.destinationDefinitionId), Workspace(workspaceId))),
      )
    } returns true

    val workspace = mockk<StandardWorkspace>()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace
    every { workspace.organizationId } returns UUID.randomUUID()
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        listOf(
          destinationDefinition.destinationDefinitionId,
          hiddenDestinationDefinition.destinationDefinitionId,
        ),
      )
    } returns
      mapOf(
        destinationDefinition.destinationDefinitionId to true,
        hiddenDestinationDefinition.destinationDefinitionId to true,
      )

    every { destinationService.listPublicDestinationDefinitions(false) } returns listOf(destinationDefinition, hiddenDestinationDefinition)
    every { destinationService.listGrantedDestinationDefinitions(workspaceId, false) } returns emptyList()
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          destinationDefinition,
        ),
        workspaceId,
      )
    } returns mapOf(destinationDefinitionVersion.actorDefinitionId to destinationDefinitionVersion)

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    val expectedIds =
      listOf<UUID?>(destinationDefinition.destinationDefinitionId)

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.destinationDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .destinationDefinitions
          .map { obj: DestinationDefinitionRead? -> obj!!.destinationDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return the right list")
  fun testListDestinationDefinitionsUsedByWorkspace() {
    val usedDestinationDefinition = generateDestinationDefinition()
    val usedDestinationDefinitionVersion = generateVersionFromDestinationDefinition(usedDestinationDefinition)

    every {
      destinationService.listDestinationDefinitionsForWorkspace(
        workspaceId,
        false,
      )
    } returns listOf(usedDestinationDefinition)
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          usedDestinationDefinition,
        ),
        workspaceId,
      )
    } returns mapOf(usedDestinationDefinitionVersion.actorDefinitionId to usedDestinationDefinitionVersion)

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = true))

    val expectedIds =
      listOf<UUID?>(usedDestinationDefinition.destinationDefinitionId)

    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.destinationDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .destinationDefinitions
          .map { obj: DestinationDefinitionRead? -> obj!!.destinationDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return all definitions when filterByUsed is false")
  fun testListDestinationDefinitionsUsedByWorkspaceWithFilterByUsedFalse() {
    val destinationDefinition2 = generateDestinationDefinition()
    val destinationDefinitionVersion2 = generateVersionFromDestinationDefinition(destinationDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every { destinationService.listPublicDestinationDefinitions(false) } returns listOf(destinationDefinition)
    every { destinationService.listGrantedDestinationDefinitions(workspaceId, false) } returns listOf(destinationDefinition2)
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          destinationDefinition,
          destinationDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        destinationDefinitionVersion.actorDefinitionId to destinationDefinitionVersion,
        destinationDefinitionVersion2.actorDefinitionId to destinationDefinitionVersion2,
      )
    val workspace = mockk<StandardWorkspace>()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace
    every { workspace.organizationId } returns UUID.randomUUID()
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.DESTINATION_CONNECTOR,
        listOf(destinationDefinition.destinationDefinitionId),
      )
    } returns mapOf(destinationDefinition.destinationDefinitionId to true)

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = false))

    val expectedIds = listOf<UUID?>(destinationDefinition.destinationDefinitionId, destinationDefinition2.destinationDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.destinationDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .destinationDefinitions
          .map { obj: DestinationDefinitionRead? -> obj!!.destinationDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listDestinationDefinitionsUsedByWorkspace should return only used definitions when filterByUsed is true")
  fun testListDestinationDefinitionsUsedByWorkspaceWithFilterByUsedTrue() {
    val usedDestinationDefinition = generateDestinationDefinition()
    val usedDestinationDefinitionVersion = generateVersionFromDestinationDefinition(usedDestinationDefinition)

    every {
      destinationService.listDestinationDefinitionsForWorkspace(
        workspaceId,
        false,
      )
    } returns listOf(usedDestinationDefinition)
    every {
      actorDefinitionVersionHelper.getDestinationVersions(
        listOf(
          usedDestinationDefinition,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        usedDestinationDefinitionVersion.actorDefinitionId to usedDestinationDefinitionVersion,
      )

    val actualDestinationDefinitionReadList =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = true))

    val expectedIds = listOf<UUID?>(usedDestinationDefinition.destinationDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualDestinationDefinitionReadList.destinationDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualDestinationDefinitionReadList
          .destinationDefinitions
          .map { obj: DestinationDefinitionRead? -> obj!!.destinationDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listPrivateDestinationDefinitions should return the right list")
  fun testListPrivateDestinationDefinitions() {
    every {
      destinationService.listGrantableDestinationDefinitions(
        workspaceId,
        false,
      )
    } returns mapOf(destinationDefinition to false).entries.toList()
    every {
      actorDefinitionService.getActorDefinitionVersions(
        listOf<UUID?>(destinationDefinition.defaultVersionId),
      )
    } returns listOf(destinationDefinitionVersion)

    val expectedDestinationDefinitionRead1 =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val expectedDestinationDefinitionOptInRead1 =
      PrivateDestinationDefinitionRead(
        destinationDefinition = expectedDestinationDefinitionRead1,
        granted = false,
      )

    val actualDestinationDefinitionOptInReadList =
      destinationDefinitionsHandler.listPrivateDestinationDefinitions(
        WorkspaceIdRequestBody(workspaceId = workspaceId),
      )

    Assertions.assertEquals(
      listOf<PrivateDestinationDefinitionRead?>(expectedDestinationDefinitionOptInRead1),
      actualDestinationDefinitionOptInReadList.destinationDefinitions,
    )
  }

  @Test
  @DisplayName("getDestinationDefinition should return the right destination")
  fun testGetDestination() {
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinition.destinationDefinitionId,
        true,
      )
    } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val actualDestinationDefinitionRead =
      destinationDefinitionsHandler.getDestinationDefinition(destinationDefinition.destinationDefinitionId, true)

    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead)
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should throw an exception for a missing grant")
  fun testGetDefinitionWithoutGrantForWorkspace() {
    every { workspaceService.workspaceCanUseDefinition(destinationDefinition.destinationDefinitionId, workspaceId) } returns false

    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        workspaceId = workspaceId,
      )

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId) }
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should throw an exception for a missing grant")
  fun testGetDefinitionWithoutGrantForScope() {
    every {
      actorDefinitionService.scopeCanUseDefinition(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      )
    } returns false
    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      )
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForWorkspace) }

    every {
      actorDefinitionService.scopeCanUseDefinition(
        destinationDefinition.destinationDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      )
    } returns false
    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      )
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { destinationDefinitionsHandler.getDestinationDefinitionForScope(actorDefinitionIdWithScopeForOrganization) }
  }

  @Test
  @DisplayName("getDestinationDefinitionForWorkspace should return the destination definition if the grant exists")
  fun testGetDefinitionWithGrantForWorkspace() {
    every { workspaceService.workspaceCanUseDefinition(destinationDefinition.destinationDefinitionId, workspaceId) } returns true
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinition.destinationDefinitionId,
        true,
      )
    } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        workspaceId = workspaceId,
      )

    val actualDestinationDefinitionRead =
      destinationDefinitionsHandler
        .getDestinationDefinitionForWorkspace(destinationDefinitionIdWithWorkspaceId)

    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionRead)
  }

  @Test
  @DisplayName("getDestinationDefinitionForScope should return the destination definition if the grant exists")
  fun testGetDefinitionWithGrantForScope() {
    every {
      actorDefinitionService.scopeCanUseDefinition(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      )
    } returns true
    every {
      actorDefinitionService.scopeCanUseDefinition(
        destinationDefinition.destinationDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      )
    } returns true
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinition.destinationDefinitionId,
        true,
      )
    } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      )

    val actualDestinationDefinitionReadForWorkspace =
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace,
      )
    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForWorkspace)

    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      )

    val actualDestinationDefinitionReadForOrganization =
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization,
      )
    Assertions.assertEquals(expectedDestinationDefinitionRead, actualDestinationDefinitionReadForOrganization)
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition")
  fun testCreateCustomDestinationDefinition() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition)

    every { uuidSupplier.get() } returns newDestinationDefinition.destinationDefinitionId

    val create =
      DestinationDefinitionCreate(
        name = newDestinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = newDestinationDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newDestinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreate =
      CustomDestinationDefinitionCreate(destinationDefinition = create, workspaceId = workspaceId)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    } returns destinationDefinitionVersion

    val expectedRead =
      DestinationDefinitionRead(
        name = newDestinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        destinationDefinitionId = newDestinationDefinition.destinationDefinitionId,
        icon = null,
        protocolVersion = DEFAULT_PROTOCOL_VERSION,
        custom = true,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage = KotlinReleaseStage.CUSTOM,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newDestinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val actualRead = destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate)

    Assertions.assertEquals(expectedRead, actualRead)
    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    }
    verify(exactly = 1) {
      destinationService.writeCustomConnectorMetadata(
        newDestinationDefinition
          .withCustom(true)
          .withDefaultVersionId(null)
          .withIconUrl(null),
        destinationDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }

    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("createCustomDestinationDefinition should correctly create a destinationDefinition for a workspace and organization using scopes")
  fun testCreateCustomDestinationDefinitionUsingScopes() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateCustomVersionFromDestinationDefinition(destinationDefinition)

    every { uuidSupplier.get() } returns newDestinationDefinition.destinationDefinitionId

    val create =
      DestinationDefinitionCreate(
        name = newDestinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = newDestinationDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newDestinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreateForWorkspace =
      CustomDestinationDefinitionCreate(
        destinationDefinition = create,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
        workspaceId = null,
      ) // scopeType and scopeId should be sufficient to resolve to the expected workspaceId

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    } returns destinationDefinitionVersion

    val expectedRead =
      DestinationDefinitionRead(
        name = newDestinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        destinationDefinitionId = newDestinationDefinition.destinationDefinitionId,
        icon = null,
        protocolVersion = DEFAULT_PROTOCOL_VERSION,
        custom = true,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage = KotlinReleaseStage.CUSTOM,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newDestinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val actualRead =
      destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForWorkspace)

    Assertions.assertEquals(expectedRead, actualRead)
    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    }
    verify(exactly = 1) {
      destinationService.writeCustomConnectorMetadata(
        newDestinationDefinition
          .withCustom(true)
          .withDefaultVersionId(null)
          .withIconUrl(null),
        destinationDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }

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
    // .scopeType(KotlinScopeType.ORGANIZATION);
    //
    // when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.dockerRepository,
    // create.dockerImageTag,
    // create.documentationUrl,
    // null))
    // .thenReturn(destinationDefinitionVersion);
    //
    // destinationDefinitionsHandler.createCustomDestinationDefinition(customCreateForOrganization);
    //
    // verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.dockerRepository,
    // create.dockerImageTag,
    // create.documentationUrl,
    // null);
    // verify(destinationService).writeCustomConnectorMetadata(newDestinationDefinition.withCustom(true).withDefaultVersionId(null),
    // destinationDefinitionVersion, organizationId, ScopeType.ORGANIZATION);
    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "createCustomDestinationDefinition should not create a destinationDefinition " +
        "if defaultDefinitionVersionFromCreate throws unsupported protocol version error"
    ),
  )
  fun testCreateCustomDestinationDefinitionShouldCheckProtocolVersion() {
    val newDestinationDefinition = generateDestinationDefinition()
    val destinationDefinitionVersion = generateVersionFromDestinationDefinition(newDestinationDefinition)

    val create =
      DestinationDefinitionCreate(
        name = newDestinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = newDestinationDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newDestinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreate =
      CustomDestinationDefinitionCreate(destinationDefinition = create, workspaceId = workspaceId)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    } throws UnsupportedProtocolVersionException(Version("1.0.0"), Version("1.0.0"), Version("1.0.0"))
    Assertions.assertThrows(
      UnsupportedProtocolVersionException::class.java,
    ) { destinationDefinitionsHandler.createCustomDestinationDefinition(customCreate) }

    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    }
    verify(exactly = 0) {
      destinationService.writeCustomConnectorMetadata(
        any(),
        any(),
        any(),
        any(),
      )
    }

    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("updateDestinationDefinition should correctly update a destinationDefinition")
  fun testUpdateDestination() {
    every {
      airbyteCompatibleConnectorsValidator.validate(
        any(),
        any(),
      )
    } returns ConnectorPlatformCompatibilityValidationResult(true, "")

    val newDockerImageTag = "averydifferenttag"
    val updatedDestination =
      clone(destinationDefinition).withDefaultVersionId(null)
    val updatedDestinationDefVersion =
      generateVersionFromDestinationDefinition(updatedDestination)
        .withDockerImageTag(newDockerImageTag)
        .withVersionId(UUID.randomUUID())

    val persistedUpdatedDestination =
      clone<StandardDestinationDefinition>(updatedDestination).withDefaultVersionId(updatedDestinationDefVersion.versionId)

    every { destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId) } returnsMany
      listOf(
        destinationDefinition, // Call at the beginning of the method
        persistedUpdatedDestination, // Call after we've persisted
      )

    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        destinationDefinitionVersion,
        ActorType.DESTINATION,
        newDockerImageTag,
        destinationDefinition.custom,
        workspaceId,
      )
    } returns updatedDestinationDefVersion

    val breakingChanges: MutableList<ActorDefinitionBreakingChange> = generateBreakingChangesFromDestinationDefinition(updatedDestination)
    every {
      actorDefinitionHandlerHelper.getBreakingChanges(
        updatedDestinationDefVersion,
        ActorType.DESTINATION,
      )
    } returns breakingChanges

    val destinationRead =
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate(
          destinationDefinitionId = this.destinationDefinition.destinationDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = workspaceId,
        ),
      )

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = newDockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    Assertions.assertEquals(expectedDestinationDefinitionRead, destinationRead)
    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        destinationDefinitionVersion,
        ActorType.DESTINATION,
        newDockerImageTag,
        destinationDefinition.custom,
        workspaceId,
      )
    }
    verify(exactly = 1) { actorDefinitionHandlerHelper.getBreakingChanges(updatedDestinationDefVersion, ActorType.DESTINATION) }
    verify(exactly = 1) { destinationService.writeConnectorMetadata(updatedDestination, updatedDestinationDefVersion, breakingChanges) }
    verify(exactly = 1) { supportStateUpdater.updateSupportStatesForDestinationDefinition(persistedUpdatedDestination) }
    verify(exactly = 1) { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) }
    confirmVerified(actorDefinitionHandlerHelper, supportStateUpdater)
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  fun testBuildDestinationDefinitionUpdateNameNonCustom() {
    val existingDestinationDefinition = destinationDefinition

    val newDestinationDefinition =
      destinationDefinitionsHandler.buildDestinationDefinitionUpdate(
        existingDestinationDefinition,
        name = "Some name that gets ignored",
        resourceRequirements = null,
      )

    Assertions.assertEquals(newDestinationDefinition.name, existingDestinationDefinition.name)
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  fun testBuildDestinationDefinitionUpdateNameCustom() {
    val newName = "My new connector name"
    val existingCustomDestinationDefinition = generateDestinationDefinition().withCustom(true)

    val newDestinationDefinition =
      destinationDefinitionsHandler.buildDestinationDefinitionUpdate(
        existingCustomDestinationDefinition,
        name = newName,
        resourceRequirements = null,
      )

    Assertions.assertEquals(newDestinationDefinition.name, newName)
  }

  @Test
  @DisplayName(
    (
      "updateDestinationDefinition should not update a destinationDefinition " +
        "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error"
    ),
  )
  fun testOutOfProtocolRangeUpdateDestination() {
    every {
      airbyteCompatibleConnectorsValidator.validate(
        any(),
        any(),
      )
    } returns ConnectorPlatformCompatibilityValidationResult(true, "")
    every { destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId) } returns destinationDefinition
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinition.destinationDefinitionId,
        true,
      )
    } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion
    val currentDestination =
      destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.destinationDefinitionId, true)
    val currentTag = currentDestination.dockerImageTag
    val newDockerImageTag = "averydifferenttagforprotocolversion"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        destinationDefinitionVersion,
        ActorType.DESTINATION,
        newDockerImageTag,
        destinationDefinition.custom,
        workspaceId,
      )
    } throws UnsupportedProtocolVersionException(Version("1.0.0"), Version("1.0.0"), Version("1.0.0"))

    Assertions.assertThrows(UnsupportedProtocolVersionException::class.java) {
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate(
          destinationDefinitionId = this.destinationDefinition.destinationDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = workspaceId,
        ),
      )
    }

    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        destinationDefinitionVersion,
        ActorType.DESTINATION,
        newDockerImageTag,
        destinationDefinition.custom,
        workspaceId,
      )
    }
    verify(exactly = 0) {
      destinationService.writeConnectorMetadata(
        any(),
        any(),
        any(),
      )
    }

    verify(exactly = 1) { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) }
    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "updateDestinationDefinition should not update a destinationDefinition " +
        "if Airbyte version is unsupported"
    ),
  )
  fun testUnsupportedAirbyteVersionUpdateDestination() {
    every {
      actorDefinitionHandlerHelper.validateVersionSupport(
        any(),
        "12.4.0",
        ActorType.DESTINATION,
      )
    } throws BadRequestProblem()
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinition.destinationDefinitionId,
        true,
      )
    } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion
    val currentDestination =
      destinationDefinitionsHandler
        .getDestinationDefinition(destinationDefinition.destinationDefinitionId, true)
    val currentTag = currentDestination.dockerImageTag
    val newDockerImageTag = "12.4.0"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    Assertions.assertThrows(BadRequestProblem::class.java) {
      destinationDefinitionsHandler.updateDestinationDefinition(
        DestinationDefinitionUpdate(
          destinationDefinitionId = this.destinationDefinition.destinationDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = UUID.randomUUID(),
        ),
      )
    }
    verify(exactly = 0) {
      destinationService.writeConnectorMetadata(
        any(),
        any(),
        any(),
      )
    }
    verify(exactly = 1) { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) }
    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("deleteDestinationDefinition should correctly delete a sourceDefinition")
  fun testDeleteDestinationDefinition() {
    val updatedDestinationDefinition = clone<StandardDestinationDefinition>(this.destinationDefinition).withTombstone(true)
    val newDestinationDefinition =
      DestinationRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        destinationId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = jsonNode(emptyMap<String, Any>()),
        name = "destination",
        destinationName = destinationDefinition.name,
        createdAt = 1L,
      )

    every { destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId) } returns destinationDefinition
    every { destinationHandler.listDestinationsForDestinationDefinition(destinationDefinition.destinationDefinitionId) } returns
      DestinationReadList(destinations = listOf(newDestinationDefinition))

    Assertions.assertFalse(destinationDefinition.tombstone)

    destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinition.destinationDefinitionId)

    verify(exactly = 1) { destinationHandler.deleteDestination(newDestinationDefinition) }
    verify(exactly = 1) { destinationService.updateStandardDestinationDefinition(updatedDestinationDefinition) }
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspace should correctly create a workspace grant")
  fun testGrantDestinationDefinitionToWorkspace() {
    every { destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId) } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val expectedPrivateDestinationDefinitionRead =
      PrivateDestinationDefinitionRead(
        destinationDefinition = expectedDestinationDefinitionRead,
        granted = true,
      )

    val actualPrivateDestinationDefinitionRead =
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope(
          actorDefinitionId = destinationDefinition.destinationDefinitionId,
          scopeId = workspaceId,
          scopeType = KotlinScopeType.WORKSPACE,
        ),
      )

    Assertions.assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead)
    verify(exactly = 1) {
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }
  }

  @Test
  @DisplayName("grantDestinationDefinitionToWorkspaceOrOrganization should correctly create an organization grant")
  fun testGrantDestinationDefinitionToOrganization() {
    every { destinationService.getStandardDestinationDefinition(destinationDefinition.destinationDefinitionId) } returns destinationDefinition
    every { actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId) } returns destinationDefinitionVersion

    val expectedDestinationDefinitionRead =
      DestinationDefinitionRead(
        destinationDefinitionId = destinationDefinition.destinationDefinitionId,
        name = destinationDefinition.name,
        dockerRepository = destinationDefinitionVersion.dockerRepository,
        dockerImageTag = destinationDefinitionVersion.dockerImageTag,
        documentationUrl = URI(destinationDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        protocolVersion = destinationDefinitionVersion.protocolVersion,
        supportLevel =
          KotlinSupportLevel
            .decode(destinationDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(destinationDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(destinationDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = destinationDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = destinationDefinitionVersion.language,
        supportsDataActivation = destinationDefinitionVersion.supportsDataActivation,
      )

    val expectedPrivateDestinationDefinitionRead =
      PrivateDestinationDefinitionRead(
        destinationDefinition = expectedDestinationDefinitionRead,
        granted = true,
      )

    val actualPrivateDestinationDefinitionRead =
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope(
          actorDefinitionId = destinationDefinition.destinationDefinitionId,
          scopeId = organizationId,
          scopeType = KotlinScopeType.ORGANIZATION,
        ),
      )

    Assertions.assertEquals(expectedPrivateDestinationDefinitionRead, actualPrivateDestinationDefinitionRead)
    verify(exactly = 1) {
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        destinationDefinition.destinationDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }
  }

  @Test
  @DisplayName("revokeDestinationDefinitionFromWorkspace should correctly delete a workspace grant")
  fun testRevokeDestinationDefinitionFromWorkspace() {
    destinationDefinitionsHandler.revokeDestinationDefinition(
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      ),
    )
    verify(exactly = 1) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        destinationDefinition.destinationDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }

    destinationDefinitionsHandler.revokeDestinationDefinition(
      ActorDefinitionIdWithScope(
        actorDefinitionId = destinationDefinition.destinationDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      ),
    )
    verify(exactly = 1) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        destinationDefinition.destinationDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }
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
              jsonNode(mapOf("key" to "val")),
            ),
          ).withTombstone(false)
          .withProtocolVersion("0.2.2")
          .withSupportLevel(SupportLevel.COMMUNITY)
          .withAbInternal(AbInternal().withSl(100L))
          .withReleaseStage(ReleaseStage.ALPHA)
          .withReleaseDate(todayDateString)
          .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))
          .withLanguage("java")
      every { remoteDefinitionsProvider.getDestinationDefinitions() } returns mutableListOf(registryDestinationDefinition!!)

      val destinationDefinitionReadList = destinationDefinitionsHandler.listLatestDestinationDefinitions().destinationDefinitions
      Assertions.assertEquals(1, destinationDefinitionReadList.size)

      val destinationDefinitionRead = destinationDefinitionReadList[0]
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
      every { remoteDefinitionsProvider.getDestinationDefinitions() } throws RuntimeException()
      Assertions.assertEquals(0, destinationDefinitionsHandler.listLatestDestinationDefinitions().destinationDefinitions.size)
    }
  }

  companion object {
    private val todayDateString = LocalDate.now().toString()
    private const val DEFAULT_PROTOCOL_VERSION = "0.2.0"
    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-presto/latest/icon.svg"
  }
}
