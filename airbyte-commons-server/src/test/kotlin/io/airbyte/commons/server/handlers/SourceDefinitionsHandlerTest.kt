/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.model.generated.JobTypeResourceLimit
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionCreate
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionUpdate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceReadList
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
import io.airbyte.config.ConnectorRegistryEntryMetrics
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.MapperConfig
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
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
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
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.time.LocalDate
import java.util.Date
import java.util.Map
import java.util.UUID
import java.util.function.Supplier

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

  private val apiPojoConverters = ApiPojoConverters(CatalogConverter(FieldGenerator(), mutableListOf<Mapper<out MapperConfig>>()))

  @BeforeEach
  fun setUp() {
    actorDefinitionService = mock()
    uuidSupplier = mock()
    actorDefinitionHandlerHelper = mock()
    remoteDefinitionsProvider = mock()
    sourceHandler = mock()
    supportStateUpdater = mock()
    workspaceId = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    sourceDefinition = generateSourceDefinition()
    sourceDefinitionVersion = generateVersionFromSourceDefinition(sourceDefinition)
    sourceDefinitionWithOptionals = generateSourceDefinitionWithOptionals()
    sourceDefinitionVersionWithOptionals = generateSourceDefinitionVersionWithOptionals(sourceDefinitionWithOptionals)
    featureFlagClient = mock<TestClient>()
    actorDefinitionVersionHelper = mock()
    airbyteCompatibleConnectorsValidator = mock()
    licenseEntitlementChecker = mock()
    sourceService = mock()
    workspaceService = mock()

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
  @Throws(IOException::class, URISyntaxException::class)
  fun testListSourceDefinitions() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(sourceService.listStandardSourceDefinitions(false))
      .thenReturn(listOf(sourceDefinition, sourceDefinition2, sourceDefinitionWithOptionals))
    whenever(
      actorDefinitionService.getActorDefinitionVersions(
        listOf(
          sourceDefinition.defaultVersionId,
          sourceDefinition2.defaultVersionId,
          sourceDefinitionWithOptionals.defaultVersionId,
        ),
      ),
    ).thenReturn(
      listOf(
        sourceDefinitionVersion,
        sourceDefinitionVersion2,
        sourceDefinitionVersionWithOptionals,
      ),
    )

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .cdkVersion(null)
        .lastPublished(null)
        .metrics(null)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.sourceDefinitionId)
        .name(sourceDefinition2.name)
        .dockerRepository(sourceDefinitionVersion2.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion2.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion2.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion2.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion2.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion2.releaseDate))
        .cdkVersion(null)
        .lastPublished(null)
        .metrics(null)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition2.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion2.language)

    val expectedSourceDefinitionReadWithOpts =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinitionWithOptionals.sourceDefinitionId)
        .name(sourceDefinitionWithOptionals.name)
        .dockerRepository(sourceDefinitionVersionWithOptionals.dockerRepository)
        .dockerImageTag(sourceDefinitionVersionWithOptionals.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersionWithOptionals.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersionWithOptionals.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersionWithOptionals.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersionWithOptionals.releaseDate))
        .cdkVersion(sourceDefinitionVersionWithOptionals.cdkVersion)
        .lastPublished(apiPojoConverters.toOffsetDateTime(sourceDefinitionVersionWithOptionals.lastPublished))
        .metrics(sourceDefinitionWithOptionals.metrics)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersionWithOptionals.language)

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
  @Throws(IOException::class, URISyntaxException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListSourceDefinitionsForWorkspace() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      featureFlagClient.boolVariation(
        eq(HideActorDefinitionFromList),
        any(),
      ),
    ).thenReturn(false)
    whenever(sourceService.listPublicSourceDefinitions(false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition))
    whenever(sourceService.listGrantedSourceDefinitions(workspaceId, false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition2))
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      ),
    ).thenReturn(
      Map.of<UUID, ActorDefinitionVersion?>(
        sourceDefinitionVersion.actorDefinitionId,
        sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId,
        sourceDefinitionVersion2,
      ),
    )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      ),
    )
    whenever(
      licenseEntitlementChecker.checkEntitlements(
        any(),
        eq(Entitlement.SOURCE_CONNECTOR),
        eq(listOf(sourceDefinition.sourceDefinitionId)),
      ),
    ).thenReturn(mapOf(sourceDefinition.sourceDefinitionId to true))

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.sourceDefinitionId)
        .name(sourceDefinition2.name)
        .dockerRepository(sourceDefinitionVersion2.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion2.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion2.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion2.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion2.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion2.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition2.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion2.language)

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    Assertions.assertEquals(
      listOf<SourceDefinitionRead?>(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
      actualSourceDefinitionReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list, filtering out hidden connectors")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListSourceDefinitionsForWorkspaceWithHiddenConnectors() {
    val hiddenSourceDefinition = generateSourceDefinition()
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      featureFlagClient.boolVariation(
        eq(HideActorDefinitionFromList),
        any(),
      ),
    ).thenReturn(false)
    whenever(
      featureFlagClient.boolVariation(
        HideActorDefinitionFromList,
        Multi(listOf(SourceDefinition(hiddenSourceDefinition.sourceDefinitionId), Workspace(workspaceId))),
      ),
    ).thenReturn(true)

    whenever(sourceService.listPublicSourceDefinitions(false))
      .thenReturn(listOf<StandardSourceDefinition>(hiddenSourceDefinition, sourceDefinition))
    whenever(sourceService.listGrantedSourceDefinitions(workspaceId, false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition2))
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      ),
    ).thenReturn(
      Map.of<UUID, ActorDefinitionVersion?>(
        sourceDefinitionVersion.actorDefinitionId,
        sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId,
        sourceDefinitionVersion2,
      ),
    )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      ),
    )
    whenever(
      licenseEntitlementChecker.checkEntitlements(
        any(),
        eq(Entitlement.SOURCE_CONNECTOR),
        eq(
          listOf(
            hiddenSourceDefinition.sourceDefinitionId,
            sourceDefinition.sourceDefinitionId,
          ),
        ),
      ),
    ).thenReturn(
      mapOf(
        sourceDefinition.sourceDefinitionId to true,
        hiddenSourceDefinition.sourceDefinitionId to true,
      ),
    )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .stream()
          .map<UUID?> { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsForWorkspace should return the right list, filtering out unentitled connectors")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListSourceDefinitionsForWorkspaceWithUnentitledConnectors() {
    val unentitledSourceDefinition = generateSourceDefinition()
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      featureFlagClient.boolVariation(
        eq(HideActorDefinitionFromList),
        any(),
      ),
    ).thenReturn(false)
    whenever(sourceService.listPublicSourceDefinitions(false))
      .thenReturn(listOf<StandardSourceDefinition>(unentitledSourceDefinition, sourceDefinition))
    whenever(sourceService.listGrantedSourceDefinitions(workspaceId, false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition2))
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      ),
    ).thenReturn(
      Map.of<UUID, ActorDefinitionVersion?>(
        sourceDefinitionVersion.actorDefinitionId,
        sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId,
        sourceDefinitionVersion2,
      ),
    )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      ),
    )

    whenever(
      licenseEntitlementChecker.checkEntitlements(
        any(),
        eq(Entitlement.SOURCE_CONNECTOR),
        eq(
          listOf(
            unentitledSourceDefinition.sourceDefinitionId,
            sourceDefinition.sourceDefinitionId,
          ),
        ),
      ),
    ).thenReturn(
      mapOf(
        sourceDefinition.sourceDefinitionId to true,
        unentitledSourceDefinition.sourceDefinitionId to false,
      ),
    )

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .stream()
          .map<UUID?> { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listPrivateSourceDefinitions should return the right list")
  @Throws(IOException::class, URISyntaxException::class)
  fun testListPrivateSourceDefinitions() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      sourceService.listGrantableSourceDefinitions(
        workspaceId,
        false,
      ),
    ).thenReturn(
      listOf<MutableMap.MutableEntry<StandardSourceDefinition, Boolean>>(
        Map.entry<StandardSourceDefinition, Boolean>(sourceDefinition, false),
        Map.entry<StandardSourceDefinition, Boolean>(sourceDefinition2, true),
      ),
    )
    whenever(
      actorDefinitionService.getActorDefinitionVersions(
        listOf(
          sourceDefinition.defaultVersionId,
          sourceDefinition2.defaultVersionId,
        ),
      ),
    ).thenReturn(listOf(sourceDefinitionVersion, sourceDefinitionVersion2))

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.sourceDefinitionId)
        .name(sourceDefinition2.name)
        .dockerRepository(sourceDefinitionVersion2.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion2.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion2.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion2.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion2.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion2.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition2.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion2.language)

    val expectedSourceDefinitionOptInRead1 =
      PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead1).granted(false)

    val expectedSourceDefinitionOptInRead2 =
      PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead2).granted(true)

    val actualSourceDefinitionOptInReadList =
      sourceDefinitionsHandler.listPrivateSourceDefinitions(
        WorkspaceIdRequestBody().workspaceId(workspaceId),
      )

    Assertions.assertEquals(
      listOf<PrivateSourceDefinitionRead?>(expectedSourceDefinitionOptInRead1, expectedSourceDefinitionOptInRead2),
      actualSourceDefinitionOptInReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("getSourceDefinition should return the right source")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGetSourceDefinition() {
    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val actualSourceDefinitionRead =
      sourceDefinitionsHandler.getSourceDefinition(sourceDefinition.sourceDefinitionId, true)

    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead)
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should throw an exception for a missing grant")
  @Throws(IOException::class)
  fun testGetDefinitionWithoutGrantForWorkspace() {
    whenever(workspaceService.workspaceCanUseDefinition(sourceDefinition.sourceDefinitionId, workspaceId))
      .thenReturn(false)

    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .workspaceId(workspaceId)

    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId) }
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should throw an exception for a missing grant")
  @Throws(IOException::class)
  fun testGetDefinitionWithoutGrantForScope() {
    whenever(
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      ),
    ).thenReturn(
      false,
    )
    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForWorkspace) }

    whenever(
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      ),
    ).thenReturn(
      false,
    )
    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)
    Assertions.assertThrows(
      IdNotFoundKnownException::class.java,
    ) { sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScopeForOrganization) }
  }

  @Test
  @DisplayName("getSourceDefinitionForWorkspace should return the source definition if the grant exists")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGetDefinitionWithGrantForWorkspace() {
    whenever(workspaceService.workspaceCanUseDefinition(sourceDefinition.sourceDefinitionId, workspaceId))
      .thenReturn(true)
    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .workspaceId(workspaceId)

    val actualSourceDefinitionRead =
      sourceDefinitionsHandler
        .getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId)

    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead)
  }

  @Test
  @DisplayName("getSourceDefinitionForScope should return the source definition if the grant exists")
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    URISyntaxException::class,
    ConfigNotFoundException::class,
  )
  fun testGetDefinitionWithGrantForScope() {
    whenever(
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        workspaceId,
        ScopeType.WORKSPACE.value(),
      ),
    ).thenReturn(true)
    whenever(
      actorDefinitionService.scopeCanUseDefinition(
        sourceDefinition.sourceDefinitionId,
        organizationId,
        ScopeType.ORGANIZATION.value(),
      ),
    ).thenReturn(
      true,
    )
    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId, true))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val actorDefinitionIdWithScopeForWorkspace =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)

    val actualSourceDefinitionReadForWorkspace =
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForWorkspace,
      )
    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForWorkspace)

    val actorDefinitionIdWithScopeForOrganization =
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)

    val actualSourceDefinitionReadForOrganization =
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScopeForOrganization,
      )
    Assertions.assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionReadForOrganization)
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition")
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomSourceDefinition() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition)

    whenever<UUID?>(uuidSupplier.get()).thenReturn(newSourceDefinition.sourceDefinitionId)

    val create =
      SourceDefinitionCreate()
        .name(newSourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(newSourceDefinition.icon)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newSourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreate =
      CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId)

    whenever(
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        customCreate.workspaceId,
      ),
    ).thenReturn(sourceDefinitionVersion)

    val expectedRead =
      SourceDefinitionRead()
        .name(newSourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .sourceDefinitionId(newSourceDefinition.sourceDefinitionId)
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.COMMUNITY)
        .releaseStage(io.airbyte.api.model.generated.ReleaseStage.CUSTOM)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newSourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val actualRead = sourceDefinitionsHandler.createCustomSourceDefinition(customCreate)

    Assertions.assertEquals(expectedRead, actualRead)
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.dockerRepository,
      create.dockerImageTag,
      create.documentationUrl,
      customCreate.workspaceId,
    )
    verify(sourceService).writeCustomConnectorMetadata(
      newSourceDefinition
        .withCustom(true)
        .withDefaultVersionId(null)
        .withIconUrl(null),
      sourceDefinitionVersion,
      workspaceId,
      ScopeType.WORKSPACE,
    )

    verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("createCustomSourceDefinition should correctly create a sourceDefinition for a workspace and organization using scopes")
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomSourceDefinitionUsingScopes() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateCustomVersionFromSourceDefinition(sourceDefinition)

    whenever<UUID?>(uuidSupplier.get()).thenReturn(newSourceDefinition.sourceDefinitionId)

    val create =
      SourceDefinitionCreate()
        .name(newSourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(newSourceDefinition.icon)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newSourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreateForWorkspace =
      CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .workspaceId(null) // scopeType and scopeId should be sufficient to resolve to the expected workspaceId

    whenever(
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        workspaceId,
      ),
    ).thenReturn(sourceDefinitionVersion)

    val expectedRead =
      SourceDefinitionRead()
        .name(newSourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .sourceDefinitionId(newSourceDefinition.sourceDefinitionId)
        .icon(null)
        .protocolVersion(DEFAULT_PROTOCOL_VERSION)
        .custom(true)
        .supportLevel(io.airbyte.api.model.generated.SupportLevel.COMMUNITY)
        .releaseStage(io.airbyte.api.model.generated.ReleaseStage.CUSTOM)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newSourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val actualRead =
      sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForWorkspace)

    Assertions.assertEquals(expectedRead, actualRead)
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.dockerRepository,
      create.dockerImageTag,
      create.documentationUrl,
      workspaceId,
    )
    verify(sourceService).writeCustomConnectorMetadata(
      newSourceDefinition
        .withCustom(true)
        .withDefaultVersionId(null)
        .withIconUrl(null),
      sourceDefinitionVersion,
      workspaceId,
      ScopeType.WORKSPACE,
    )

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
    // .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION);
    //
    // when(actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(create.getDockerRepository(),
    // create.getDockerImageTag(),
    // create.getDocumentationUrl(),
    // null))
    // .thenReturn(sourceDefinitionVersion);
    //
    // sourceDefinitionsHandler.createCustomSourceDefinition(customCreateForOrganization);
    //
    // verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(create.getDockerRepository(),
    // create.getDockerImageTag(),
    // create.getDocumentationUrl(),
    // null);
    // verify(sourceService).writeCustomConnectorMetadata(newSourceDefinition.withCustom(true).withDefaultVersionId(null),
    // sourceDefinitionVersion, organizationId, ScopeType.ORGANIZATION);
    verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "createCustomSourceDefinition should not create a sourceDefinition " +
        "if defaultDefinitionVersionFromCreate throws unsupported protocol version error"
    ),
  )
  @Throws(URISyntaxException::class, IOException::class)
  fun testCreateCustomSourceDefinitionShouldCheckProtocolVersion() {
    val newSourceDefinition = generateSourceDefinition()
    val sourceDefinitionVersion = generateVersionFromSourceDefinition(newSourceDefinition)

    val create =
      SourceDefinitionCreate()
        .name(newSourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(newSourceDefinition.icon)
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(newSourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        )

    val customCreate =
      CustomSourceDefinitionCreate()
        .sourceDefinition(create)
        .workspaceId(workspaceId)

    whenever<ActorDefinitionVersion?>(
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromCreate(
        create.dockerRepository,
        create.dockerImageTag,
        create.documentationUrl,
        customCreate.workspaceId,
      ),
    ).thenThrow(UnsupportedProtocolVersionException::class.java)
    whenever<UUID?>(uuidSupplier.get()).thenReturn(UUID.randomUUID())
    Assertions.assertThrows<UnsupportedProtocolVersionException?>(
      UnsupportedProtocolVersionException::class.java,
    ) { sourceDefinitionsHandler.createCustomSourceDefinition(customCreate) }

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromCreate(
      create.dockerRepository,
      create.dockerImageTag,
      create.documentationUrl,
      customCreate.workspaceId,
    )
    verify(sourceService, never()).writeCustomConnectorMetadata(
      any(),
      any(),
      any(),
      any(),
    )

    verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    URISyntaxException::class,
    ConfigNotFoundException::class,
  )
  fun testUpdateSource() {
    whenever(
      airbyteCompatibleConnectorsValidator.validate(
        any(),
        any(),
      ),
    ).thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))

    val newDockerImageTag = "averydifferenttag"
    val updatedSource =
      clone(sourceDefinition).withDefaultVersionId(null)
    val updatedSourceDefVersion =
      generateVersionFromSourceDefinition(updatedSource)
        .withDockerImageTag(newDockerImageTag)
        .withVersionId(UUID.randomUUID())

    val persistedUpdatedSource =
      clone<StandardSourceDefinition>(updatedSource).withDefaultVersionId(updatedSourceDefVersion.versionId)

    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(sourceDefinition) // Call at the beginning of the method
      .thenReturn(persistedUpdatedSource) // Call after we've persisted

    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    whenever(
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      ),
    ).thenReturn(updatedSourceDefVersion)

    val breakingChanges = generateBreakingChangesFromSourceDefinition(updatedSource)
    whenever(
      actorDefinitionHandlerHelper.getBreakingChanges(
        updatedSourceDefVersion,
        ActorType.SOURCE,
      ),
    ).thenReturn(breakingChanges)

    val sourceRead =
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate()
          .sourceDefinitionId(this.sourceDefinition.sourceDefinitionId)
          .dockerImageTag(newDockerImageTag)
          .workspaceId(workspaceId),
      )

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(newDockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    Assertions.assertEquals(expectedSourceDefinitionRead, sourceRead)
    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(
      sourceDefinitionVersion,
      ActorType.SOURCE,
      newDockerImageTag,
      sourceDefinition.custom,
      workspaceId,
    )
    verify(actorDefinitionHandlerHelper).getBreakingChanges(updatedSourceDefVersion, ActorType.SOURCE)
    verify(sourceService).writeConnectorMetadata(updatedSource, updatedSourceDefVersion, breakingChanges)
    verify(supportStateUpdater).updateSupportStatesForSourceDefinition(persistedUpdatedSource)
    verifyNoMoreInteractions(actorDefinitionHandlerHelper, supportStateUpdater)
  }

  @Test
  @DisplayName("does not update the name of a non-custom connector definition")
  fun testBuildSourceDefinitionUpdateNameNonCustom() {
    val existingSourceDefinition = sourceDefinition

    val sourceDefinitionUpdate =
      SourceDefinitionUpdate()
        .sourceDefinitionId(existingSourceDefinition.sourceDefinitionId)
        .name("Some name that gets ignored")

    val newSourceDefinition =
      sourceDefinitionsHandler.buildSourceDefinitionUpdate(existingSourceDefinition, sourceDefinitionUpdate)

    Assertions.assertEquals(newSourceDefinition.name, existingSourceDefinition.name)
  }

  @Test
  @DisplayName("updates the name of a custom connector definition")
  fun testBuildSourceDefinitionUpdateNameCustom() {
    val newName = "My new connector name"
    val existingCustomSourceDefinition = generateSourceDefinition().withCustom(true)

    val sourceDefinitionUpdate =
      SourceDefinitionUpdate()
        .sourceDefinitionId(existingCustomSourceDefinition.sourceDefinitionId)
        .name(newName)

    val newSourceDefinition =
      sourceDefinitionsHandler.buildSourceDefinitionUpdate(existingCustomSourceDefinition, sourceDefinitionUpdate)

    Assertions.assertEquals(newSourceDefinition.name, newName)
  }

  @Test
  @DisplayName(
    (
      "updateSourceDefinition should not update a sourceDefinition " +
        "if defaultDefinitionVersionFromUpdate throws unsupported protocol version error"
    ),
  )
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testOutOfProtocolRangeUpdateSource() {
    whenever<ConnectorPlatformCompatibilityValidationResult?>(
      airbyteCompatibleConnectorsValidator.validate(any(), any()),
    ).thenReturn(ConnectorPlatformCompatibilityValidationResult(true, ""))
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever<StandardSourceDefinition?>(
      sourceService.getStandardSourceDefinition(
        sourceDefinition.sourceDefinitionId,
        true,
      ),
    ).thenReturn(sourceDefinition)
    whenever<ActorDefinitionVersion?>(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)
    val currentSource =
      sourceDefinitionsHandler
        .getSourceDefinition(sourceDefinition.sourceDefinitionId, true)
    val currentTag = currentSource.dockerImageTag
    val newDockerImageTag = "averydifferenttagforprotocolversion"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    whenever<ActorDefinitionVersion?>(
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        sourceDefinitionVersion,
        ActorType.SOURCE,
        newDockerImageTag,
        sourceDefinition.custom,
        workspaceId,
      ),
    ).thenThrow(UnsupportedProtocolVersionException::class.java)

    Assertions.assertThrows<UnsupportedProtocolVersionException?>(
      UnsupportedProtocolVersionException::class.java,
    ) {
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate()
          .sourceDefinitionId(this.sourceDefinition.sourceDefinitionId)
          .dockerImageTag(newDockerImageTag)
          .workspaceId(workspaceId),
      )
    }

    verify(actorDefinitionHandlerHelper).defaultDefinitionVersionFromUpdate(
      sourceDefinitionVersion,
      ActorType.SOURCE,
      newDockerImageTag,
      sourceDefinition.custom,
      workspaceId,
    )
    verify(sourceService, never()).writeConnectorMetadata(
      any(),
      any(),
      any(),
    )

    verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName(
    (
      "updateSourceDefinition should not update a sourceDefinition " +
        "if Airbyte version is unsupported"
    ),
  )
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun testUnsupportedAirbyteVersionUpdateSource() {
    whenever(
      airbyteCompatibleConnectorsValidator.validate(
        any(),
        eq("12.4.0"),
      ),
    ).thenReturn(ConnectorPlatformCompatibilityValidationResult(false, ""))
    whenever(
      sourceService.getStandardSourceDefinition(
        sourceDefinition.sourceDefinitionId,
        true,
      ),
    ).thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)
    val currentSource = sourceDefinitionsHandler.getSourceDefinition(sourceDefinition.sourceDefinitionId, true)
    val currentTag = currentSource.dockerImageTag
    val newDockerImageTag = "12.4.0"
    Assertions.assertNotEquals(newDockerImageTag, currentTag)

    Assertions.assertThrows<BadRequestProblem?>(BadRequestProblem::class.java) {
      sourceDefinitionsHandler.updateSourceDefinition(
        SourceDefinitionUpdate()
          .sourceDefinitionId(this.sourceDefinition.sourceDefinitionId)
          .dockerImageTag(newDockerImageTag),
      )
    }
    verify(sourceService, never()).writeConnectorMetadata(
      any(),
      any(),
      any(),
    )

    verifyNoMoreInteractions(actorDefinitionHandlerHelper)
  }

  @Test
  @DisplayName("deleteSourceDefinition should correctly delete a sourceDefinition")
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDeleteSourceDefinition() {
    val updatedSourceDefinition = clone<StandardSourceDefinition>(this.sourceDefinition).withTombstone(true)
    val newSourceDefinition = SourceRead()

    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever(sourceHandler.listSourcesForSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(SourceReadList().sources(mutableListOf<@Valid SourceRead?>(newSourceDefinition)))

    Assertions.assertFalse(sourceDefinition.tombstone)

    sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinition.sourceDefinitionId)

    verify(sourceHandler).deleteSource(newSourceDefinition)
    verify(sourceService).updateStandardSourceDefinition(updatedSourceDefinition)
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create a workspace grant")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGrantSourceDefinitionToWorkspace() {
    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedPrivateSourceDefinitionRead =
      PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true)

    val actualPrivateSourceDefinitionRead =
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope()
          .actorDefinitionId(sourceDefinition.sourceDefinitionId)
          .scopeId(workspaceId)
          .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE),
      )

    Assertions.assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead)
    verify(actorDefinitionService)
      .writeActorDefinitionWorkspaceGrant(sourceDefinition.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)
  }

  @Test
  @DisplayName("grantSourceDefinitionToWorkspace should correctly create an organization grant")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, URISyntaxException::class)
  fun testGrantSourceDefinitionToOrganization() {
    whenever(sourceService.getStandardSourceDefinition(sourceDefinition.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId))
      .thenReturn(sourceDefinitionVersion)

    val expectedSourceDefinitionRead =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedPrivateSourceDefinitionRead =
      PrivateSourceDefinitionRead().sourceDefinition(expectedSourceDefinitionRead).granted(true)

    val actualPrivateSourceDefinitionRead =
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        ActorDefinitionIdWithScope()
          .actorDefinitionId(sourceDefinition.sourceDefinitionId)
          .scopeId(organizationId)
          .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION),
      )

    Assertions.assertEquals(expectedPrivateSourceDefinitionRead, actualPrivateSourceDefinitionRead)
    verify(actorDefinitionService).writeActorDefinitionWorkspaceGrant(
      sourceDefinition.sourceDefinitionId,
      organizationId,
      ScopeType.ORGANIZATION,
    )
  }

  @Test
  @DisplayName("revokeSourceDefinition should correctly delete a workspace grant and organization grant")
  @Throws(IOException::class)
  fun testRevokeSourceDefinition() {
    sourceDefinitionsHandler.revokeSourceDefinition(
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(workspaceId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE),
    )
    verify(actorDefinitionService)
      .deleteActorDefinitionWorkspaceGrant(sourceDefinition.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)

    sourceDefinitionsHandler.revokeSourceDefinition(
      ActorDefinitionIdWithScope()
        .actorDefinitionId(sourceDefinition.sourceDefinitionId)
        .scopeId(organizationId)
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION),
    )
    verify(actorDefinitionService).deleteActorDefinitionWorkspaceGrant(
      sourceDefinition.sourceDefinitionId,
      organizationId,
      ScopeType.ORGANIZATION,
    )
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
    whenever(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(
      mutableListOf<ConnectorRegistrySourceDefinition>(
        registrySourceDefinition!!,
      ),
    )

    val expectedRead =
      sourceDefinitionsHandler.buildSourceDefinitionRead(
        ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition),
      )

    Assertions.assertEquals(expectedRead.supportLevel, io.airbyte.api.model.generated.SupportLevel.NONE)
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
      whenever(remoteDefinitionsProvider.getSourceDefinitions()).thenReturn(
        mutableListOf<ConnectorRegistrySourceDefinition>(registrySourceDefinition!!),
      )

      val sourceDefinitionReadList = sourceDefinitionsHandler.listLatestSourceDefinitions().sourceDefinitions
      Assertions.assertEquals(1, sourceDefinitionReadList.size)

      val sourceDefinitionRead = sourceDefinitionReadList.get(0)
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
      whenever(remoteDefinitionsProvider.getSourceDefinitions()).thenThrow(
        RuntimeException(),
      )
      Assertions.assertEquals(0, sourceDefinitionsHandler.listLatestSourceDefinitions().sourceDefinitions.size)
    }
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return the right list")
  @Throws(IOException::class, URISyntaxException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListSourceDefinitionsUsedByWorkspace() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      featureFlagClient.boolVariation(
        eq(HideActorDefinitionFromList),
        any(),
      ),
    ).thenReturn(false)
    whenever(sourceService.listPublicSourceDefinitions(false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition))
    whenever(sourceService.listGrantedSourceDefinitions(workspaceId, false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition2))
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      ),
    ).thenReturn(
      Map.of<UUID, ActorDefinitionVersion?>(
        sourceDefinitionVersion.actorDefinitionId,
        sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId,
        sourceDefinitionVersion2,
      ),
    )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      ),
    )
    whenever(
      licenseEntitlementChecker.checkEntitlements(
        any(),
        eq(Entitlement.SOURCE_CONNECTOR),
        eq(listOf(sourceDefinition.sourceDefinitionId)),
      ),
    ).thenReturn(mapOf(sourceDefinition.sourceDefinitionId to true))

    val expectedSourceDefinitionRead1 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .name(sourceDefinition.name)
        .dockerRepository(sourceDefinitionVersion.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion.language)

    val expectedSourceDefinitionRead2 =
      SourceDefinitionRead()
        .sourceDefinitionId(sourceDefinition2.sourceDefinitionId)
        .name(sourceDefinition2.name)
        .dockerRepository(sourceDefinitionVersion2.dockerRepository)
        .dockerImageTag(sourceDefinitionVersion2.dockerImageTag)
        .documentationUrl(URI(sourceDefinitionVersion2.documentationUrl))
        .icon(ICON_URL)
        .supportLevel(
          io.airbyte.api.model.generated.SupportLevel
            .fromValue(sourceDefinitionVersion2.supportLevel.value()),
        ).releaseStage(
          io.airbyte.api.model.generated.ReleaseStage
            .fromValue(sourceDefinitionVersion2.releaseStage.value()),
        ).releaseDate(LocalDate.parse(sourceDefinitionVersion2.releaseDate))
        .resourceRequirements(
          io.airbyte.api.model.generated
            .ScopedResourceRequirements()
            ._default(
              io.airbyte.api.model.generated
                .ResourceRequirements()
                .cpuRequest(sourceDefinition2.resourceRequirements.default.cpuRequest),
            ).jobSpecific(mutableListOf<@Valid JobTypeResourceLimit?>()),
        ).language(sourceDefinitionVersion2.language)

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(false))

    Assertions.assertEquals(
      listOf<SourceDefinitionRead?>(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
      actualSourceDefinitionReadList.sourceDefinitions,
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return only used definitions when filterByUsed is true")
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testListSourceDefinitionsUsedByWorkspaceWithFilterByUsedTrue() {
    val usedSourceDefinition = generateSourceDefinition()
    val usedSourceDefinitionVersion = generateVersionFromSourceDefinition(usedSourceDefinition)

    whenever(sourceService.listSourceDefinitionsForWorkspace(workspaceId, false)).thenReturn(
      listOf(usedSourceDefinition),
    )
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          usedSourceDefinition,
        ),
        workspaceId,
      ),
    ).thenReturn(Map.of<UUID, ActorDefinitionVersion?>(usedSourceDefinitionVersion.actorDefinitionId, usedSourceDefinitionVersion))

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(true))

    val expectedIds = listOf(usedSourceDefinition.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .stream()
          .map<UUID?> { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId }
          .toList(),
      ),
    )
  }

  @Test
  @DisplayName("listSourceDefinitionsUsedByWorkspace should return all definitions when filterByUsed is false")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testListSourceDefinitionsUsedByWorkspaceWithFilterByUsedFalse() {
    val sourceDefinition2 = generateSourceDefinition()
    val sourceDefinitionVersion2 = generateVersionFromSourceDefinition(sourceDefinition2)

    whenever(
      featureFlagClient.boolVariation(
        eq(HideActorDefinitionFromList),
        any(),
      ),
    ).thenReturn(false)
    whenever(sourceService.listPublicSourceDefinitions(false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition))
    whenever(sourceService.listGrantedSourceDefinitions(workspaceId, false))
      .thenReturn(listOf<StandardSourceDefinition>(sourceDefinition2))
    whenever(
      actorDefinitionVersionHelper.getSourceVersions(
        listOf(
          sourceDefinition,
          sourceDefinition2,
        ),
        workspaceId,
      ),
    ).thenReturn(
      Map.of<UUID, ActorDefinitionVersion?>(
        sourceDefinitionVersion.actorDefinitionId,
        sourceDefinitionVersion,
        sourceDefinitionVersion2.actorDefinitionId,
        sourceDefinitionVersion2,
      ),
    )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(
      StandardWorkspace().withOrganizationId(
        UUID.randomUUID(),
      ),
    )
    whenever(
      licenseEntitlementChecker.checkEntitlements(
        any(),
        eq(Entitlement.SOURCE_CONNECTOR),
        eq(listOf(sourceDefinition.sourceDefinitionId)),
      ),
    ).thenReturn(mapOf(sourceDefinition.sourceDefinitionId to true))

    val actualSourceDefinitionReadList =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId).filterByUsed(false))

    val expectedIds =
      listOf(sourceDefinition.sourceDefinitionId, sourceDefinition2.sourceDefinitionId)
    Assertions.assertEquals(expectedIds.size, actualSourceDefinitionReadList.sourceDefinitions.size)
    Assertions.assertTrue(
      expectedIds.containsAll(
        actualSourceDefinitionReadList.sourceDefinitions
          .stream()
          .map<UUID?> { obj: SourceDefinitionRead? -> obj!!.sourceDefinitionId }
          .toList(),
      ),
    )
  }

  companion object {
    private val todayDateString = LocalDate.now().toString()
    private const val DEFAULT_PROTOCOL_VERSION = "0.2.0"
    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/source-presto/latest/icon.svg"
  }
}
