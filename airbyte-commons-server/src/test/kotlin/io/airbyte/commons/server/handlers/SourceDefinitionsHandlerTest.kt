/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomSourceDefinitionCreate
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionCreate
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionUpdate
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
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
import io.airbyte.config.ConnectorRegistryEntryMetrics
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopeType
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.SupportLevel
import io.airbyte.config.helpers.ConnectorRegistryConverters
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.ConnectorPlatformCompatibilityValidationResult
import io.airbyte.config.init.SupportStateUpdater
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
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

internal class SourceDefinitionsHandlerTest {
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceDefinition: StandardSourceDefinition
  private lateinit var sourceDefinitionWithOptionals: StandardSourceDefinition
  private lateinit var sourceDefinitionVersion: ActorDefinitionVersion
  private lateinit var sourceDefinitionVersionWithOptionals: ActorDefinitionVersion
  private lateinit var sourceDefinitionsHandler: SourceDefinitionsHandler
  private lateinit var uuidSupplier: Supplier<UUID>
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var remoteDefinitionsProvider: RemoteDefinitionsProvider
  private lateinit var sourceHandler: SourceHandler
  private lateinit var supportStateUpdater: SupportStateUpdater
  private lateinit var workspaceId: UUID
  private lateinit var organizationId: UUID
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var licenseEntitlementChecker: LicenseEntitlementChecker

  private lateinit var airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator
  private lateinit var sourceService: SourceService
  private lateinit var workspaceService: WorkspaceService

  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf()))

  @BeforeEach
  fun setUp() {
    actorDefinitionService = mockk()
    uuidSupplier = mockk()
    actorDefinitionHandlerHelper = mockk()
    remoteDefinitionsProvider = mockk()
    sourceHandler = mockk()
    supportStateUpdater = mockk()
    workspaceId = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    sourceDefinition = generateSourceDefinition()
    sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition)
    sourceDefinitionWithOptionals = generateSourceDefinitionWithOptionals()
    sourceDefinitionVersionWithOptionals = generateSourceDefinitionVersionWithOptionals(sourceDefinitionWithOptionals)
    featureFlagClient = mockk<TestClient>()
    actorDefinitionVersionHelper = mockk()
    airbyteCompatibleConnectorsValidator = mockk()
    licenseEntitlementChecker = mockk()
    sourceService = mockk()
    workspaceService = mockk()

    sourceDefinitionsHandler =
      SourceDefinitionsHandler(
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
        licenseEntitlementChecker,
        apiPojoConverters,
      )

    every { actorDefinitionService.writeActorDefinitionWorkspaceGrant(any(), any(), any()) } just Runs
    every { actorDefinitionService.deleteActorDefinitionWorkspaceGrant(any(), any(), any()) } just Runs
    every { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) } just Runs
    every { sourceService.writeCustomConnectorMetadata(any(), any(), any(), any()) } just Runs
    every { sourceService.writeConnectorMetadata(any(), any(), any()) } just Runs
    every { sourceService.updateStandardSourceDefinition(any()) } just Runs
    every { sourceHandler.deleteSource(any<SourceRead>()) } just Runs
    every { supportStateUpdater.updateSupportStatesForSourceDefinition(any()) } just Runs
  }

  private fun generateSourceDefinition(): StandardSourceDefinition =
    StandardSourceDefinition()
      .withSourceDefinitionId(UUID.randomUUID())
      .withDefaultVersionId(UUID.randomUUID())
      .withName("presto")
      .withIcon("rss.svg")
      .withIconUrl(ICON_URL)
      .withTombstone(false)
      .withResourceRequirements(
        ScopedResourceRequirements()
          .withDefault(ResourceRequirements().withCpuRequest("2")),
      )

  private fun generateVersionFromSourceDefinition(sourceDefinition: StandardSourceDefinition): ActorDefinitionVersion {
    val spec =
      ConnectorSpecification().withConnectionSpecification(
        jsonNode(mapOf<String?, String?>("foo" to "bar")),
      )

    return ActorDefinitionVersion()
      .withVersionId(sourceDefinition.defaultVersionId)
      .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
      .withDocumentationUrl("https://netflix.com")
      .withDockerRepository("dockerstuff")
      .withDockerImageTag("12.3")
      .withSpec(spec)
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.ALPHA)
      .withReleaseDate(todayDateString)
      .withAllowedHosts(AllowedHosts().withHosts(mutableListOf<String?>("host1", "host2")))
      .withSuggestedStreams(SuggestedStreams().withStreams(mutableListOf<String?>("stream1", "stream2")))
      .withLanguage("python")
  }

  private fun generateBreakingChangesFromSourceDefinition(sourceDefinition: StandardSourceDefinition): List<ActorDefinitionBreakingChange> {
    val breakingChange =
      ActorDefinitionBreakingChange()
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersion(Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21")
    return mutableListOf(breakingChange)
  }

  private fun generateCustomVersionFromSourceDefinition(sourceDefinition: StandardSourceDefinition): ActorDefinitionVersion =
    generateVersionFromSourceDefinition(sourceDefinition)
      .withProtocolVersion(DEFAULT_PROTOCOL_VERSION)
      .withReleaseDate(null)
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.CUSTOM)
      .withAllowedHosts(null)
      .withLanguage("manifest-only")

  private fun generateSourceDefinitionWithOptionals(): StandardSourceDefinition {
    val metrics =
      ConnectorRegistryEntryMetrics().withAdditionalProperty("all", JSONB.valueOf("{'all': {'usage': 'high'}}"))
    return generateSourceDefinition().withMetrics(metrics)
  }

  private fun generateSourceDefinitionVersionWithOptionals(sourceDefinition: StandardSourceDefinition): ActorDefinitionVersion =
    generateVersionFromSourceDefinition(sourceDefinition)
      .withCdkVersion("python:1.2.3")
      .withLastPublished(Date())

  @Test
  @DisplayName("listSourceDefinition should return the right list")
  fun testListSourceDefinitions() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every { sourceService.listStandardSourceDefinitions(false) } returns listOf(sourceDefinition, sourceDefinition2, sourceDefinitionWithOptionals)
    every {
      actorDefinitionService.getActorDefinitionVersions(
        listOf(
          sourceDefinition.defaultVersionId,
          sourceDefinition2.defaultVersionId,
          sourceDefinitionWithOptionals.defaultVersionId,
        ),
      )
    } returns
      listOf(
        sourceDefinitionVersion,
        sourceDefinitionVersion2,
        sourceDefinitionVersionWithOptionals,
      )

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        cdkVersion = null,
        lastPublished = null,
        metrics = null,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition2.sourceDefinitionId,
        name = sourceDefinition2.name,
        dockerRepository = sourceDefinitionVersion2.dockerRepository,
        dockerImageTag = sourceDefinitionVersion2.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion2.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion2.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion2.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion2.releaseDate),
        cdkVersion = null,
        lastPublished = null,
        metrics = null,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition2.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion2.language,
      )

    val expectedSourceDefinitionReadWithOpts =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinitionWithOptionals.sourceDefinitionId,
        name = sourceDefinitionWithOptionals.name,
        dockerRepository = sourceDefinitionVersionWithOptionals.dockerRepository,
        dockerImageTag = sourceDefinitionVersionWithOptionals.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersionWithOptionals.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersionWithOptionals.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersionWithOptionals.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersionWithOptionals.releaseDate),
        cdkVersion = sourceDefinitionVersionWithOptionals.cdkVersion,
        lastPublished = apiPojoConverters.toOffsetDateTime(sourceDefinitionVersionWithOptionals.lastPublished),
        metrics = sourceDefinitionWithOptionals.metrics,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersionWithOptionals.language,
      )

    val actualSourceDefinitionReadList = sourceDefinitionsHandler.listSourceDefinitions()

    Assertions.assertEquals(
      listOf<SourceDefinitionRead?>(
        expectedSourceDefinitionRead1,
        expectedSourceDefinitionRead2,
        expectedSourceDefinitionReadWithOpts,
      ),
      actualSourceDefinitionReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list")
  fun testListSourceDefinitionsForWorkspace() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(sourceDefinition)
    every { sourceService.listGrantedSourceDefinitions(workspaceId, false) } returns listOf(sourceDefinition2)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        sourceDefinitionVersion.actorDefinitionId to sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId to sourceDefinitionVersion2,
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      )
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        listOf(sourceDefinition.sourceDefinitionId),
      )
    } returns mapOf(sourceDefinition.sourceDefinitionId to true)

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition2.sourceDefinitionId,
        name = sourceDefinition2.name,
        dockerRepository = sourceDefinitionVersion2.dockerRepository,
        dockerImageTag = sourceDefinitionVersion2.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion2.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion2.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion2.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion2.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition2.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion2.language,
      )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    Assertions.assertEquals(
      listOf<SourceDefinitionRead?>(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
      actualSourceDefinitionReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  fun testListSourceDefinitionsForWorkspaceWithHiddenConnectors() {
    val hiddenSourceDefinition = generateSourceDefinition()
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        Multi(listOf(SourceDefinition(hiddenSourceDefinition.sourceDefinitionId), Workspace(workspaceId))),
      )
    } returns true

    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(hiddenSourceDefinition, sourceDefinition)
    every { sourceService.listGrantedSourceDefinitions(workspaceId, false) } returns listOf(sourceDefinition2)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        sourceDefinitionVersion.actorDefinitionId to sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId to sourceDefinitionVersion2,
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      )
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        listOf(
          hiddenSourceDefinition.sourceDefinitionId,
          sourceDefinition.sourceDefinitionId,
        ),
      )
    } returns
      mapOf(
        sourceDefinition.sourceDefinitionId to true,
        hiddenSourceDefinition.sourceDefinitionId to true,
      )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .map { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list, filtering out unentitled connectors")
  fun testListSourceDefinitionsForWorkspaceWithUnentitledConnectors() {
    val unentitledSourceDefinition = generateSourceDefinition()
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(unentitledSourceDefinition, sourceDefinition)
    every { sourceService.listGrantedSourceDefinitions(workspaceId, false) } returns listOf(sourceDefinition2)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        sourceDefinitionVersion.actorDefinitionId to sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId to sourceDefinitionVersion2,
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      )

    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        listOf(
          unentitledSourceDefinition.sourceDefinitionId,
          sourceDefinition.sourceDefinitionId,
        ),
      )
    } returns
      mapOf(
        sourceDefinition.sourceDefinitionId to true,
        unentitledSourceDefinition.sourceDefinitionId to false,
      )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .map { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listPrivateSourceDefinitions should return the right list")
  fun testListPrivateSourceDefinitions() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      sourceService.listGrantableSourceDefinitions(
        workspaceId,
        false,
      )
    } returns
      mapOf(
        sourceDefinition to false,
        sourceDefinition2 to true,
      ).entries.toList()
    every {
      actorDefinitionService.getActorDefinitionVersions(
        listOf(
          sourceDefinition.defaultVersionId,
          sourceDefinition2.defaultVersionId,
        ),
      )
    } returns listOf(sourceDefinitionVersion, sourceDefinitionVersion2)

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition2.sourceDefinitionId,
        name = sourceDefinition2.name,
        dockerRepository = sourceDefinitionVersion2.dockerRepository,
        dockerImageTag = sourceDefinitionVersion2.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion2.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion2.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion2.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion2.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition2.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion2.language,
      )

    val expectedSourceDefinitionOptInRead1 =
      PrivateSourceDefinitionRead(sourceDefinition = expectedSourceDefinitionRead1, granted = false)

    val expectedSourceDefinitionOptInRead2 =
      PrivateSourceDefinitionRead(sourceDefinition = expectedSourceDefinitionRead2, granted = true)

    val actualSourceDefinitionOptInReadList =
      sourceDefinitionsHandler.listPrivateSourceDefinitions(
        WorkspaceIdRequestBody(workspaceId = workspaceId),
      )

    Assertions.assertEquals(
      listOf<PrivateSourceDefinitionRead?>(expectedSourceDefinitionOptInRead1, expectedSourceDefinitionOptInRead2),
      actualSourceDefinitionOptInReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("getSourceDefinition should return the right source")
  fun testGetSourceDefinition() {
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true) } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val actualSourceDefinitionRead =
      sourceDefinitionsHandler.getSourceDefinition(sourceDefinition.sourceDefinitionId, true)

    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead)
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should throw an exception for a missing grant")
  fun testGetDefinitionWithoutGrantForWorkspace() {
    every { workspaceService.workspaceCanUseDefinition(sourceDefinition.sourceDefinitionId, workspaceId) } returns false

    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        workspaceId = workspaceId,
      )

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId) }
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should throw an exception for a missing grant")
  fun testGetDefinitionWithoutGrantForScope() {
    every {
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      )
    } returns false
    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      )
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForWorkspace) }

    every {
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      )
    } returns false
    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      )
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForOrganization) }
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should return the source definition if the grant exists")
  fun testGetDefinitionWithGrantForWorkspace() {
    every { workspaceService.workspaceCanUseDefinition(sourceDefinition.sourceDefinitionId, workspaceId) } returns true
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true) } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        workspaceId = workspaceId,
      )

    val actualSourceDefinitionRead =
      sourceDefinitionsHandler
        .getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId)

    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead)
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should return the source definition if the grant exists")
  fun testGetDefinitionWithGrantForScope() {
    every {
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      )
    } returns true
    every {
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      )
    } returns true
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true) } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      )

    val actualSourceDefinitionReadForWorkspace =
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace,
      )
    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForWorkspace)

    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      )

    val actualSourceDefinitionReadForOrganization =
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization,
      )
    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForOrganization)
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition")
  fun testCreateCustomSourceDefinition() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition)

    every { uuidSupplier.get() } returns newSourceDefinition.sourceDefinitionId

    val create =
      SourceDefinitionCreate(
        name = newSourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = newSourceDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newSourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreate =
      CustomSourceDefinitionCreate(sourceDefinition = create, workspaceId = workspaceId)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    } returns sourceDefinitionVersion

    val expectedRead =
      SourceDefinitionRead(
        name = newSourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        sourceDefinitionId = newSourceDefinition.sourceDefinitionId,
        icon = null,
        protocolVersion = DEFAULT_PROTOCOL_VERSION,
        custom = true,
        supportLevel = KotlinSupportLevel.COMMUNITY,
        releaseStage = KotlinReleaseStage.CUSTOM,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newSourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreate)

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
      sourceService.writeCustomConnectorMetadata(
        newSourceDefinition
          .withCustom(true)
          .withDefaultVersionId(null)
          .withIconUrl(null),
        sourceDefinitionVersion,
        workspaceId,
        ScopeType.WORKSPACE,
      )
    }

    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition for a workspace and organization using scopes")
  fun testCreateCustomSourceDefinitionUsingScopes() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition)

    every { uuidSupplier.get() } returns newSourceDefinition.sourceDefinitionId

    val create =
      SourceDefinitionCreate(
        name = newSourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = newSourceDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newSourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreateForWorkspace =
      CustomSourceDefinitionCreate(
        sourceDefinition = create,
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
    } returns sourceDefinitionVersion

    val expectedRead =
      SourceDefinitionRead(
        name = newSourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        sourceDefinitionId = newSourceDefinition.sourceDefinitionId,
        icon = null,
        protocolVersion = DEFAULT_PROTOCOL_VERSION,
        custom = true,
        supportLevel = KotlinSupportLevel.COMMUNITY,
        releaseStage = KotlinReleaseStage.CUSTOM,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newSourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val actualRead =
      sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForWorkspace)

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
      sourceService.writeCustomConnectorMetadata(
        newSourceDefinition
          .withCustom(true)
          .withDefaultVersionId(null)
          .withIconUrl(null),
        sourceDefinitionVersion,
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
    // final CustomSourceDefinitionCreate customCreateForOrganization = new
    // CustomSourceDefinitionCreate()
    // .sourceDefinition(create)
    // .scopeId(organizationId)
    // .scopeType(KotlinScopeType.ORGANIZATION);
    //
    // when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.dockerRepository,
    // create.dockerImageTag,
    // create.documentationUrl,
    // null))
    // .thenReturn(sourceDefinitionVersion);
    //
    // sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForOrganization);
    //
    // verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.dockerRepository,
    // create.dockerImageTag,
    // create.documentationUrl,
    // null);
    // verify(sourceService).writeCustomConnectorMetadata(newSourceDefinition.withCustom(true).withDefaultVersionId(null),
    // sourceDefinitionVersion, organizationId, ScopeType.ORGANIZATION);
    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "createCustomSourceDefinition should not create a sourceDefinition " +
        "if defaultDefinitionVersionFromCreate throws unsupported protocol version error"
    ),
  )
  fun testCreateCustomSourceDefinitionShouldCheckProtocolVersion() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateVersionFromSourceDefinition(newSourceDefinition)

    val create =
      SourceDefinitionCreate(
        name = newSourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = newSourceDefinition.icon,
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = newSourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
      )

    val customCreate =
      CustomSourceDefinitionCreate(sourceDefinition = create, workspaceId = workspaceId)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    } throws UnsupportedProtocolVersionException(Version("1.0.0"), Version("1.0.0"), Version("1.0.0"))

    every { uuidSupplier.get() } returns UUID.randomUUID()
    Assertions.assertThrows(
      UnsupportedProtocolVersionException::class.java,
    ) { sourceDefinitionsHandler.createCustomSourceDefinition(customCreate) }

    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      )
    }
    verify(exactly = 0) {
      sourceService.writeCustomConnectorMetadata(
        any(),
        any(),
        any(),
        any(),
      )
    }

    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  fun testUpdateSource() {
    every {
      airbyteCompatibleConnectorsValidator.validate(
        any(),
        any(),
      )
    } returns ConnectorPlatformCompatibilityValidationResult(true, "")

    val newDockerImageTag = "averydifferenttag"
    val updatedSource =
      clone(sourceDefinition).withDefaultVersionId(null)
    val updatedSourceDefVersion =
      generateVersionFromSourceDefinition(updatedSource)
        .withDockerImageTag(newDockerImageTag)
        .withVersionId(UUID.randomUUID())

    val persistedUpdatedSource =
      clone<StandardSourceDefinition>(updatedSource).withDefaultVersionId(updatedSourceDefVersion.versionId)

    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId) } returnsMany
      listOf(
        sourceDefinition, // Call at the beginning of the method
        persistedUpdatedSource, // Call after we've persisted
      )

    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      )
    } returns updatedSourceDefVersion

    val breakingChanges = generateBreakingChangesFromSourceDefinition(updatedSource)
    every {
      actorDefinitionHandlerHelper.getBreakingChanges(
        updatedSourceDefVersion,
        ActorType.SOURCE,
      )
    } returns breakingChanges

    val sourceRead =
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate(
          sourceDefinitionId = this.sourceDefinition.sourceDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = workspaceId,
        ),
      )

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = newDockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    Assertions.assertEquals(expectedSourceDefinitionRead, sourceRead)
    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      )
    }
    verify(exactly = 1) { actorDefinitionHandlerHelper.getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE) }
    verify(exactly = 1) { sourceService.writeConnectorMetadata(updatedSource, updatedSourceDefVersion, breakingChanges) }
    verify(exactly = 1) { supportStateUpdater.updateSupportStatesForSourceDefinition(persistedUpdatedSource) }
    verify(exactly = 1) { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) }
    confirmVerified(actorDefinitionHandlerHelper, supportStateUpdater)
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  fun testBuildSourceDefinitionUpdateNameNonCustom() {
    val existingSourceDefinition = sourceDefinition

    val newSourceDefinition =
      sourceDefinitionsHandler.buildSourceDefinitionUpdate(
        currentSourceDefinition = existingSourceDefinition,
        name = "Some name that gets ignored",
        resourceRequirements = null,
      )

    Assertions.assertEquals(newSourceDefinition.name, existingSourceDefinition.name)
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  fun testBuildSourceDefinitionUpdateNameCustom() {
    val newName = "My new connector name"
    val existingCustomSourceDefinition = generateSourceDefinition().withCustom(true)

    val newSourceDefinition =
      sourceDefinitionsHandler.buildSourceDefinitionUpdate(
        currentSourceDefinition = existingCustomSourceDefinition,
        name = newName,
        resourceRequirements = null,
      )

    Assertions.assertEquals(newSourceDefinition.name, newName)
  }

  @Test
  @DisplayName(
    (
      "updateSourceDefinition should not update a sourceDefinition " +
        "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error"
    ),
  )
  fun testOutOfProtocolRangeUpdateSource() {
    every { airbyteCompatibleConnectorsValidator.validate(any(), any()) } returns ConnectorPlatformCompatibilityValidationResult(true, "")
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId) } returns sourceDefinition
    every {
      sourceService.getStandardSourceDefinition(
        sourceDefinition.sourceDefinitionId,
        true,
      )
    } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion
    val currentSource =
      sourceDefinitionsHandler
        .getSourceDefinition(sourceDefinition.sourceDefinitionId, true)
    val currentTag = currentSource.dockerImageTag
    val newDockerImageTag = "averydifferenttagforprotocolversion"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    every {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      )
    } throws UnsupportedProtocolVersionException(Version("1.0.0"), Version("1.0.0"), Version("1.0.0"))

    Assertions.assertThrows(
      UnsupportedProtocolVersionException::class.java,
    ) {
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate(
          sourceDefinitionId = this.sourceDefinition.sourceDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = workspaceId,
        ),
      )
    }

    verify(exactly = 1) {
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      )
    }
    verify(exactly = 0) {
      sourceService.writeConnectorMetadata(
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
      "updateSourceDefinition should not update a sourceDefinition " +
        "if Airbyte version is unsupported"
    ),
  )
  fun testUnsupportedAirbyteVersionUpdateSource() {
    every {
      actorDefinitionHandlerHelper.validateVersionSupport(
        any(),
        "12.4.0",
        ActorType.SOURCE,
      )
    } throws BadRequestProblem()
    every {
      sourceService.getStandardSourceDefinition(
        sourceDefinition.sourceDefinitionId,
        true,
      )
    } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion
    val currentSource = sourceDefinitionsHandler.getSourceDefinition(sourceDefinition.sourceDefinitionId, true)
    val currentTag = currentSource.dockerImageTag
    val newDockerImageTag = "12.4.0"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    Assertions.assertThrows(BadRequestProblem::class.java) {
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate(
          sourceDefinitionId = this.sourceDefinition.sourceDefinitionId,
          dockerImageTag = newDockerImageTag,
          workspaceId = UUID.randomUUID(),
        ),
      )
    }
    verify(exactly = 0) {
      sourceService.writeConnectorMetadata(
        any(),
        any(),
        any(),
      )
    }

    verify(exactly = 1) { actorDefinitionHandlerHelper.validateVersionSupport(any(), any(), any()) }
    confirmVerified(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("deleteSourceDefinition should correctly delete a sourceDefinition")
  fun testDeleteSourceDefinition() {
    val updatedSourceDefinition = clone<StandardSourceDefinition>(this.sourceDefinition).withTombstone(true)
    val newSourceDefinition =
      SourceRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        sourceId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = jsonNode(emptyMap<String, Any>()),
        name = "source",
        sourceName = sourceDefinition.name,
        createdAt = 1L,
      )

    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId) } returns sourceDefinition
    every { sourceHandler.listSourcesForSourceDefinition(sourceDefinition.sourceDefinitionId) } returns
      SourceReadList(sources = listOf(newSourceDefinition))

    Assertions.assertFalse(sourceDefinition.tombstone)

    sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinition.sourceDefinitionId)

    verify(exactly = 1) { sourceHandler.deleteSource(newSourceDefinition) }
    verify(exactly = 1) { sourceService.updateStandardSourceDefinition(updatedSourceDefinition) }
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create a workspace grant")
  fun testGrantSourceDefinitionToWorkspace() {
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId) } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedPrivateSourceDefinitionRead =
      PrivateSourceDefinitionRead(sourceDefinition = expectedSourceDefinitionRead, granted = true)

    val actualPrivateSourceDefinitionRead =
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope(
          actorDefinitionId = sourceDefinition.sourceDefinitionId,
          scopeId = workspaceId,
          scopeType = KotlinScopeType.WORKSPACE,
        ),
      )

    Assertions.assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead)
    verify(exactly = 1) {
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(sourceDefinition.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)
    }
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create an organization grant")
  fun testGrantSourceDefinitionToOrganization() {
    every { sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId) } returns sourceDefinition
    every { actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId) } returns sourceDefinitionVersion

    val expectedSourceDefinitionRead =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedPrivateSourceDefinitionRead =
      PrivateSourceDefinitionRead(sourceDefinition = expectedSourceDefinitionRead, granted = true)

    val actualPrivateSourceDefinitionRead =
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope(
          actorDefinitionId = sourceDefinition.sourceDefinitionId,
          scopeId = organizationId,
          scopeType = KotlinScopeType.ORGANIZATION,
        ),
      )

    Assertions.assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead)
    verify(exactly = 1) {
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }
  }

  @Test
  @DisplayName("revokeSourceDefinition should correctly delete a workspace grant and organization grant")
  fun testRevokeSourceDefinition() {
    sourceDefinitionsHandler.revokeSourceDefinition(
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = workspaceId,
        scopeType = KotlinScopeType.WORKSPACE,
      ),
    )
    verify(exactly = 1) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(sourceDefinition.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)
    }

    sourceDefinitionsHandler.revokeSourceDefinition(
      ActorDefinitionIdWithScope(
        actorDefinitionId = sourceDefinition.sourceDefinitionId,
        scopeId = organizationId,
        scopeType = KotlinScopeType.ORGANIZATION,
      ),
    )
    verify(exactly = 1) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION,
      )
    }
  }

  @Test
  @DisplayName("should transform support level none to none")
  fun testNoneSupportLevel() {
    val registrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("some-source")
        .withDocumentationUrl("https://airbyte.com")
        .withDockerRepository("dockerrepo")
        .withDockerImageTag("1.2.4")
        .withIcon("source.svg")
        .withSpec(
          ConnectorSpecification().withConnectionSpecification(
            jsonNode(mapOf<String?, String?>("key" to "val")),
          ),
        ).withTombstone(false)
        .withSupportLevel(SupportLevel.NONE)
        .withAbInternal(AbInternal().withSl(100L))
        .withReleaseStage(ReleaseStage.ALPHA)
        .withReleaseDate(todayDateString)
        .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))
        .withLanguage("python")
    every { remoteDefinitionsProvider.getSourceDefinitions() } returns
      mutableListOf(
        registrySourceDefinition!!,
      )

    val expectedRead =
      sourceDefinitionsHandler.buildSourceDefinitionRead(
        ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition),
      )

    Assertions.assertEquals(expectedRead.supportLevel, KotlinSupportLevel.NONE)
  }

  @Nested
  @DisplayName("listLatest")
  internal inner class ListLatest {
    @Test
    @DisplayName("should return the latest list")
    fun testCorrect() {
      val registrySourceDefinition =
        ConnectorRegistrySourceDefinition()
          .withSourceDefinitionId(UUID.randomUUID())
          .withName("some-source")
          .withDocumentationUrl("https://airbyte.com")
          .withDockerRepository("dockerrepo")
          .withDockerImageTag("1.2.4")
          .withIcon("source.svg")
          .withSpec(
            ConnectorSpecification().withConnectionSpecification(
              jsonNode(mapOf<String?, String?>("key" to "val")),
            ),
          ).withTombstone(false)
          .withSupportLevel(SupportLevel.COMMUNITY)
          .withAbInternal(AbInternal().withSl(100L))
          .withReleaseStage(ReleaseStage.ALPHA)
          .withReleaseDate(todayDateString)
          .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))
          .withLanguage("python")
      every { remoteDefinitionsProvider.getSourceDefinitions() } returns mutableListOf(registrySourceDefinition!!)

      val sourceDefinitionReadList = sourceDefinitionsHandler.listLatestSourceDefinitions().sourceDefinitions
      Assertions.assertEquals(1, sourceDefinitionReadList.size)

      val sourceDefinitionRead = sourceDefinitionReadList[0]
      val expectedRead =
        sourceDefinitionsHandler.buildSourceDefinitionRead(
          ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
          ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition),
        )

      Assertions.assertEquals(expectedRead, sourceDefinitionRead)
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    fun testHttpTimeout() {
      every { remoteDefinitionsProvider.getSourceDefinitions() } throws RuntimeException()
      Assertions.assertEquals(0, sourceDefinitionsHandler.listLatestSourceDefinitions().sourceDefinitions.size)
    }
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return the right list")
  fun testListSourceDefinitionsUsedByWorkspace() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(sourceDefinition)
    every { sourceService.listGrantedSourceDefinitions(workspaceId, false) } returns listOf(sourceDefinition2)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        sourceDefinitionVersion.actorDefinitionId to sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId to sourceDefinitionVersion2,
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      )
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        listOf(sourceDefinition.sourceDefinitionId),
      )
    } returns mapOf(sourceDefinition.sourceDefinitionId to true)

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition.sourceDefinitionId,
        name = sourceDefinition.name,
        dockerRepository = sourceDefinitionVersion.dockerRepository,
        dockerImageTag = sourceDefinitionVersion.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion.language,
      )

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead(
        sourceDefinitionId = sourceDefinition2.sourceDefinitionId,
        name = sourceDefinition2.name,
        dockerRepository = sourceDefinitionVersion2.dockerRepository,
        dockerImageTag = sourceDefinitionVersion2.dockerImageTag,
        documentationUrl = URI(sourceDefinitionVersion2.documentationUrl),
        icon = ICON_URL,
        supportLevel =
          KotlinSupportLevel
            .decode(sourceDefinitionVersion2.supportLevel.value()),
        releaseStage =
          KotlinReleaseStage
            .decode(sourceDefinitionVersion2.releaseStage.value()),
        releaseDate = LocalDate.parse(sourceDefinitionVersion2.releaseDate),
        resourceRequirements =
          KotlinScopedResourceRequirements(
            default =
              KotlinResourceRequirements(
                cpuRequest = sourceDefinition2.resourceRequirements.default.cpuRequest,
              ),
            jobSpecific = emptyList(),
          ),
        language = sourceDefinitionVersion2.language,
      )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = false))

    Assertions.assertEquals(
      listOf<SourceDefinitionRead?>(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
      actualSourceDefinitionReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return only used definitions when filterByUsed is true")
  fun testListSourceDefinitionsUsedByWorkspaceWithFilterByUsedTrue() {
    val usedSourceDefinition = generateSourceDefinition()
    val usedSourceDefinitionVersion = generateVersionFromSourceDefinition(usedSourceDefinition)

    every { sourceService.listSourceDefinitionsForWorkspace(workspaceId, false) } returns listOf(usedSourceDefinition)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          usedSourceDefinition,
        ),
        workspaceId,
      )
    } returns mapOf(usedSourceDefinitionVersion.actorDefinitionId to usedSourceDefinitionVersion)

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = true))

    val expectedIds = listOf(usedSourceDefinition.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .map { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId },
      ),
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return all definitions when filterByUsed is false")
  fun testListSourceDefinitionsUsedByWorkspaceWithFilterByUsedFalse() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    every {
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        any(),
      )
    } returns false
    every { sourceService.listPublicSourceDefinitions(false) } returns listOf(sourceDefinition)
    every { sourceService.listGrantedSourceDefinitions(workspaceId, false) } returns listOf(sourceDefinition2)
    every {
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      )
    } returns
      mapOf(
        sourceDefinitionVersion.actorDefinitionId to sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId to sourceDefinitionVersion2,
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      )
    every {
      licenseEntitlementChecker.checkEntitlements(
        any(),
        Entitlement.SOURCE_CONNECTOR,
        listOf(sourceDefinition.sourceDefinitionId),
      )
    } returns mapOf(sourceDefinition.sourceDefinitionId to true)

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody(workspaceId = workspaceId, filterByUsed = false))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .map { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId },
      ),
    )
  }

  companion object {
    private val todayDateString = LocalDate.now().toString()
    private const val DEFAULT_PROTOCOL_VERSION = "0.2.0"
    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/source-presto/latest/icon.svg"
  }
}
