package io.airbyte.connector.rollout.shared

import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Schedule
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.CustomerTier
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

class RolloutActorFinderTest {
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val connectionService = mockk<ConnectionService>()
  private val jobService = mockk<JobService>()
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val organizationCustomerAttributesService = mockk<OrganizationCustomerAttributesService>()
  private val rolloutActorFinder =
    RolloutActorFinder(
      actorDefinitionVersionUpdater,
      connectionService,
      jobService,
      scopedConfigurationService,
      sourceService,
      destinationService,
      organizationCustomerAttributesService,
    )

  companion object {
    private const val TARGET_PERCENTAGE = 50
    private val SOURCE_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val DESTINATION_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val RELEASE_CANDIDATE_VERSION_ID = UUID.randomUUID()
    private val SOURCE_ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID()
    private val DESTINATION_ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID()

    private val ORGANIZATION_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION = UUID.randomUUID()

    private val ORGANIZATION_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_SOURCE = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_SOURCE = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION = UUID.randomUUID()

    private val CONFIG_SCOPE_MAP =
      mapOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_SOURCE to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_SOURCE,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_SOURCE,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_SOURCE to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_SOURCE,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_SOURCE,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2,
              ),
          ),
      )
    val SOURCE_ACTOR_IDS =
      setOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE,
        ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_SOURCE,
        ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_SOURCE,
      )
    val DESTINATION_ACTOR_IDS =
      setOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
        ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION,
        ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION,
      )

    val MOCK_CONNECTION_SYNCS: List<StandardSync> =
      CONFIG_SCOPE_MAP.map { actor ->
        StandardSync().apply {
          connectionId = UUID.randomUUID()
          sourceId = if (actor.key in SOURCE_ACTOR_IDS) actor.key else null
          destinationId = if (actor.key in DESTINATION_ACTOR_IDS) actor.key else null
          createdAt = Instant.now().toEpochMilli()
        }
      }

    val JOBS: List<Job> =
      MOCK_CONNECTION_SYNCS.map { connection ->
        Job(
          0,
          ConfigType.SYNC,
          "scope",
          JobConfig().apply {
            sync =
              JobSyncConfig().apply {
                sourceDockerImageIsDefault = true
                sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                destinationDockerImageIsDefault = true
                destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
              }
          },
          emptyList(),
          JobStatus.SUCCEEDED,
          connection.createdAt ?: 0L,
          connection.createdAt ?: 0L,
          connection.createdAt ?: 0L,
        )
      }

    private fun createMockConnectorRollout(actorDefinitionId: UUID): ConnectorRollout {
      return ConnectorRollout().apply {
        this.id = id
        this.actorDefinitionId = actorDefinitionId
        this.releaseCandidateVersionId = RELEASE_CANDIDATE_VERSION_ID
        this.initialVersionId = UUID.randomUUID()
        this.state = ConnectorEnumRolloutState.INITIALIZED
        this.initialRolloutPct = 10L
        this.finalTargetRolloutPct = TARGET_PERCENTAGE.toLong()
        this.hasBreakingChanges = false
        this.rolloutStrategy = ConnectorEnumRolloutStrategy.MANUAL
        this.maxStepWaitTimeMins = 60L
        this.createdAt = OffsetDateTime.now().toEpochSecond()
        this.updatedAt = OffsetDateTime.now().toEpochSecond()
        this.expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond()
      }
    }

    @JvmStatic
    fun actorDefinitionIds() = listOf(SOURCE_ACTOR_DEFINITION_ID, DESTINATION_ACTOR_DEFINITION_ID)
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorSelectionInfo`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns JOBS
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), TARGET_PERCENTAGE)

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any())
      jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      assertEquals(SOURCE_ACTOR_IDS.toSet().size * TARGET_PERCENTAGE / 100, actorSelectionInfo.actorIdsToPin.size)
    } else {
      assertEquals(DESTINATION_ACTOR_IDS.toSet().size * TARGET_PERCENTAGE / 100, actorSelectionInfo.actorIdsToPin.size)
    }
    assertEquals(8, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(2, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorSelectionInfo with null percentage`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns JOBS
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), null)

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any())
      jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(0, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(8, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(0, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @Test
  fun `test getTargetTotalToPin`() {
    // No eligible actors, nothing previously pinned, no targetPercentage
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(0, 0, 1))
    // No eligible actors, nothing previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(0, 0, 1))
    // No eligible actors, one previously pinned, no targetPercentage
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(0, 1, 0))
    // No eligible actors, one previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(0, 1, 1))

    // 1 eligible, 0 previously pinned
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(1, 0, 1))
    // 1 eligible, one previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(1, 1, 1))

    // 2 eligible, 0 previously pinned, targetPercentage 1
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(2, 0, 1))
    // 2 eligible, 0 previously pinned, targetPercentage 100
    assertEquals(2, rolloutActorFinder.getTargetTotalToPin(2, 0, 100))
    // 2 eligible, 1 previously pinned, targetPercentage 1
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(2, 1, 1))
    // 2 eligible, 1 previously pinned, targetPercentage 50
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(2, 1, 50))
    // 2 eligible, 1 previously pinned, targetPercentage 70
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(2, 1, 70))
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getSyncInfoForPinnedActors`(actorDefinitionId: UUID) {
    val connectorRolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(connectorRolloutId)

    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
      every {
        connectionService.listConnectionsBySources(
          any(),
          any(),
          any(),
        )
      } returns MOCK_CONNECTION_SYNCS.filter { it.sourceId in SOURCE_ACTOR_IDS }
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
      every {
        connectionService.listConnectionsByDestinations(any(), any(), any())
      } returns MOCK_CONNECTION_SYNCS.filter { it.destinationId in DESTINATION_ACTOR_IDS }
    }
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns
      MOCK_CONNECTION_SYNCS.map { connection ->
        Job(
          0,
          ConfigType.SYNC,
          "scope",
          JobConfig().apply {
            sync =
              JobSyncConfig().apply {
                sourceDockerImageIsDefault = true
                sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                destinationDockerImageIsDefault = true
                destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
              }
          },
          emptyList(),
          JobStatus.SUCCEEDED,
          connection.createdAt ?: 0L,
          connection.createdAt ?: 0L,
          connection.createdAt ?: 0L,
        )
      }

    val syncInfo = rolloutActorFinder.getSyncInfoForPinnedActors(connectorRollout)

    // Two actors are pinned per actor type
    assertEquals(2, syncInfo.size)

    verify {
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any())

      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        connectionService.listConnectionsBySources(any(), any(), any())
      } else {
        connectionService.listConnectionsByDestinations(any(), any(), any())
      }
    }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorJobInfo`(actorDefinitionId: UUID) {
    val actorType = if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION
    val connectorRolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(connectorRolloutId)

    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns JOBS

    val jobInfo =
      rolloutActorFinder.getActorJobInfo(
        connectorRollout,
        MOCK_CONNECTION_SYNCS,
        actorType,
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(connectorRollout.createdAt), ZoneOffset.UTC),
        connectorRollout.releaseCandidateVersionId,
      )

    assertEquals(4, jobInfo.size)

    verify { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test jobDefinitionVersionIdEq`(actorDefinitionId: UUID) {
    val actorType: ActorType
    val actorDefinitionVersionId: UUID
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      actorType = ActorType.SOURCE
      actorDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
    } else {
      actorType = ActorType.DESTINATION
      actorDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
    }
    val job1 =
      Job(
        1,
        ConfigType.SYNC,
        "scope",
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
              destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
      )

    val job2 =
      Job(
        2,
        ConfigType.SYNC,
        "scope",
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDefinitionVersionId = UUID.randomUUID()
              destinationDefinitionVersionId = UUID.randomUUID()
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
      )

    assertEquals(true, rolloutActorFinder.jobDefinitionVersionIdEq(actorType, job1, actorDefinitionVersionId))
    assertEquals(false, rolloutActorFinder.jobDefinitionVersionIdEq(actorType, job2, actorDefinitionVersionId))
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test jobDockerImageIsDefault`(actorDefinitionId: UUID) {
    val actorType = if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION
    val job1 =
      Job(
        1,
        ConfigType.SYNC,
        "scope",
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDockerImageIsDefault = true
              destinationDockerImageIsDefault = true
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
      )

    val job2 =
      Job(
        2,
        ConfigType.SYNC,
        "scope",
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDockerImageIsDefault = false
              destinationDockerImageIsDefault = false
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
      )

    assertEquals(true, rolloutActorFinder.jobDockerImageIsDefault(actorType, job1))
    assertEquals(false, rolloutActorFinder.jobDockerImageIsDefault(actorType, job2))
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorIdsToPin is rounded up`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns JOBS
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), 1)

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any())
      jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(8, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(1, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorIdsToPin with previously pinned`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
      every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
        listOf(
          ScopedConfiguration().apply {
            id = UUID.randomUUID()
            key = "key1"
            value = RELEASE_CANDIDATE_VERSION_ID.toString()
            resourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE
            resourceType = ConfigResourceType.SOURCE
            scopeId = UUID.randomUUID()
            scopeType = ConfigScopeType.ACTOR
            originType = ConfigOriginType.CONNECTOR_ROLLOUT
          },
        )
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
      every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
        listOf(
          ScopedConfiguration().apply {
            id = UUID.randomUUID()
            key = "key1"
            value = RELEASE_CANDIDATE_VERSION_ID.toString()
            resourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION
            resourceType = ConfigResourceType.DESTINATION
            scopeId = UUID.randomUUID()
            scopeType = ConfigScopeType.ACTOR
            originType = ConfigOriginType.CONNECTOR_ROLLOUT
          },
        )
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet() -
      setOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
      )
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns JOBS
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), 1)

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any())
      jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    // We already exceed the target percentage so shouldn't pin something new
    assertEquals(0, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(8, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(0, actorSelectionInfo.nNewPinned)
    assertEquals(1, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorType`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }

    rolloutActorFinder.getActorType(actorDefinitionId)

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
    }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getActorType throws`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } throws ConfigNotFoundException("", "Not found")
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } throws ConfigNotFoundException("", "Not found")
    }

    assertThrows<IllegalStateException> { rolloutActorFinder.getActorType(UUID.randomUUID()) }

    verify {
      sourceService.getStandardSourceDefinition(any())
      destinationService.getStandardDestinationDefinition(any())
    }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test filterByTier excludes organizations listed as tier 0 or 1`(actorDefinitionId: UUID) {
    val organizationTiers =
      mapOf(
        ORGANIZATION_ID_1 to CustomerTier.TIER_0,
        ORGANIZATION_ID_2 to CustomerTier.TIER_2,
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns organizationTiers

    val candidates =
      listOf(
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to null),
        ),
      )

    val filteredCandidates = rolloutActorFinder.filterByTier(candidates)

    assertEquals(2, filteredCandidates.size)
    assertTrue(filteredCandidates.any { it.scopeMap[ConfigScopeType.ORGANIZATION] == ORGANIZATION_ID_2 })
    assertTrue(filteredCandidates.any { it.scopeMap[ConfigScopeType.ORGANIZATION] == null })

    verify { organizationCustomerAttributesService.getOrganizationTiers() }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getNPinnedToReleaseCandidate no actors pinned`(actorDefinitionId: UUID) {
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns emptyList()

    assertEquals(
      0,
      rolloutActorFinder.getActorsPinnedToReleaseCandidate(
        createMockConnectorRollout(actorDefinitionId),
      ).size,
    )

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getNPinnedToReleaseCandidate scopes pinned to RC`(actorDefinitionId: UUID) {
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )

    assertEquals(2, rolloutActorFinder.getActorsPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)).size)

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getNPinnedToReleaseCandidate scopes not pinned to RC`(actorDefinitionId: UUID) {
    // Two scopes are pinned, but not to the RC
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )

    assertEquals(0, rolloutActorFinder.getActorsPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)).size)

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test filterByAlreadyPinned no actors pinned returns all`(actorDefinitionId: UUID) {
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values

    val candidates = rolloutActorFinder.filterByAlreadyPinned(actorDefinitionId, CONFIG_SCOPE_MAP.values)

    verify {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    }

    assertEquals(CONFIG_SCOPE_MAP.values.toSet(), candidates.toSet())
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test filterByAlreadyPinned filters pinned actors`(actorDefinitionId: UUID) {
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet() -
      setOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
      )

    val candidates = rolloutActorFinder.filterByAlreadyPinned(actorDefinitionId, CONFIG_SCOPE_MAP.values)

    assertEquals(
      CONFIG_SCOPE_MAP.filter {
        it.key != ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE && it.key != ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION
      }.values.toSet(),
      candidates.toSet(),
    )
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getSortedActorDefinitionConnections`(actorDefinitionId: UUID) {
    // Later syncs are first in the list; we'll verify that this is reversed.
    MOCK_CONNECTION_SYNCS.forEachIndexed { index, sync ->
      sync.apply {
        // The last 2 sync schedules will be manual; the two before that will be null
        if (index < MOCK_CONNECTION_SYNCS.lastIndex - 3) {
          schedule =
            Schedule().apply {
              // Assign time units in increasing order to simulate "least recent to most recent"
              timeUnit =
                when (index % 5) {
                  0 -> Schedule.TimeUnit.MINUTES
                  1 -> Schedule.TimeUnit.HOURS
                  2 -> Schedule.TimeUnit.DAYS
                  3 -> Schedule.TimeUnit.WEEKS
                  else -> Schedule.TimeUnit.MONTHS
                }
              units = (index + 1).toLong() // Increasing units for each sync to simulate future
            }
        } else if (index == MOCK_CONNECTION_SYNCS.lastIndex - 2 || index == MOCK_CONNECTION_SYNCS.lastIndex - 3) {
          // These will be the last in the sorted list
          schedule = null
        } else {
          // These will be filtered out of the list
          manual = true
        }
      }
    }

    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any()) } returns MOCK_CONNECTION_SYNCS

    val sortedConnectionSyncs =
      rolloutActorFinder.getSortedActorDefinitionConnections(
        CONFIG_SCOPE_MAP.values.map { it.id },
        actorDefinitionId,
        if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION,
      )

    // Verify one item has been removed
    assertEquals(
      3,
      sortedConnectionSyncs.size,
      "The manual sync and all syncs for the irrelevant actorIds should have been removed",
    )

    // Verify the sorted order
    for (index in 0 until sortedConnectionSyncs.size - 2) {
      val currentSync = sortedConnectionSyncs[index]
      val nextSync = sortedConnectionSyncs[index + 1]

      // Ensure the sync with 'manual = true' is excluded from the list
      assertFalse(currentSync.manual ?: false)

      // If nextSync has a schedule (i.e., it isn't the last null one)
      if (nextSync.schedule != null) {
        assertNotNull(currentSync.schedule, "Sync at index $index should have a schedule")
        assertNotNull(nextSync.schedule, "Sync at index ${index + 1} should have a schedule")

        // Ensure that the current sync's next scheduled time is earlier than the next one in the sorted list
        val currentMultiplier = getScheduleMultiplier(currentSync.schedule!!.timeUnit)
        val nextMultiplier = getScheduleMultiplier(nextSync.schedule!!.timeUnit)

        val currentTime = currentSync.schedule!!.units * currentMultiplier
        val nextTime = nextSync.schedule!!.units * nextMultiplier

        assertTrue(
          currentTime < nextTime,
          "Sync at index $index should run after or at the same time as sync at index ${index + 1}",
        )
      }
    }
    // Check if the last sync has a null schedule
    Assertions.assertNull(sortedConnectionSyncs.last().schedule, "The last sync should have a null schedule")

    verify {
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any(), any())
    }
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getSortedActorDefinitionConnectionsByActorIds`(actorDefinitionId: UUID) {
    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      every {
        connectionService.listConnectionsBySources(
          any(),
          any(),
          any(),
        )
      } returns MOCK_CONNECTION_SYNCS.filter { it.sourceId in SOURCE_ACTOR_IDS }
    } else {
      every {
        connectionService.listConnectionsByDestinations(any(), any(), any())
      } returns MOCK_CONNECTION_SYNCS.filter { it.destinationId in DESTINATION_ACTOR_IDS }
    }

    val sortedConnectionSyncs =
      rolloutActorFinder.getSortedActorDefinitionConnectionsByActorId(
        CONFIG_SCOPE_MAP.values.map { it.id },
        actorDefinitionId,
        if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION,
      )

    // Verify all items are present
    assertEquals(
      4,
      sortedConnectionSyncs.size,
      "The manual sync and all syncs for the irrelevant actorIds should have been removed",
    )

    // Verify the sorted order
    for (index in 0 until sortedConnectionSyncs.size - 2) {
      val currentSync = sortedConnectionSyncs[index]
      val nextSync = sortedConnectionSyncs[index + 1]

      // Ensure the sync with 'manual = true' is excluded from the list
      assertFalse(currentSync.manual ?: false)

      // If nextSync has a schedule (i.e., it isn't the last null one)
      if (nextSync.schedule != null) {
        assertNotNull(currentSync.schedule, "Sync at index $index should have a schedule")
        assertNotNull(nextSync.schedule, "Sync at index ${index + 1} should have a schedule")

        // Ensure that the current sync's next scheduled time is earlier than the next one in the sorted list
        val currentMultiplier = getScheduleMultiplier(currentSync.schedule!!.timeUnit)
        val nextMultiplier = getScheduleMultiplier(nextSync.schedule!!.timeUnit)

        val currentTime = currentSync.schedule!!.units * currentMultiplier
        val nextTime = nextSync.schedule!!.units * nextMultiplier

        assertTrue(
          currentTime < nextTime,
          "Sync at index $index should run after or at the same time as sync at index ${index + 1}",
        )
      }
    }
    // Check if the last sync has a null schedule
    Assertions.assertNull(sortedConnectionSyncs.last().schedule, "The last sync should have a null schedule")

    verify {
      if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
        connectionService.listConnectionsBySources(any(), any(), any())
      } else {
        connectionService.listConnectionsByDestinations(any(), any(), any())
      }
    }
  }

  private fun getScheduleMultiplier(timeUnit: Schedule.TimeUnit): Long {
    return when (timeUnit) {
      Schedule.TimeUnit.MINUTES -> 1L
      Schedule.TimeUnit.HOURS -> 60L
      Schedule.TimeUnit.DAYS -> 24 * 60L
      Schedule.TimeUnit.WEEKS -> 7 * 24 * 60L
      Schedule.TimeUnit.MONTHS -> 30 * 24 * 60L // Assuming an average month length
    }
  }

  @Test
  fun `test getFrequencyInMinutes with different schedule values`() {
    var schedule: Schedule? = null
    var expectedMinutes = Long.MAX_VALUE
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is MINUTES and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.MINUTES
        units = 10
      }
    expectedMinutes = 10
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is HOURS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.HOURS
        units = 2
      }
    expectedMinutes = 2 * 60
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is DAYS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.DAYS
        units = 3
      }
    expectedMinutes = 3 * 60 * 24
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is WEEKS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.WEEKS
        units = 1
      }
    expectedMinutes = 1 * 60 * 24 * 7
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is MONTHS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.MONTHS
        units = 1
      }
    expectedMinutes = 1 * 60 * 24 * 30
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when units is null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.DAYS
        units = null
      }
    expectedMinutes = Long.MAX_VALUE
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test filterByJobStatus`(actorDefinitionId: UUID) {
    var callCount = 0
    every { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) } answers {
      callCount++

      // Return different job lists based on the call index
      if (callCount <= 3) {
        // First 3 calls: return 2 jobs, both successful
        listOf(
          Job(
            0,
            ConfigType.SYNC,
            "scope",
            JobConfig().apply {
              sync =
                JobSyncConfig().apply {
                  sourceDockerImageIsDefault = true
                  sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                  destinationDockerImageIsDefault = true
                  destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
                }
            },
            emptyList(),
            JobStatus.SUCCEEDED,
            0L,
            0L,
            0L,
          ),
          Job(
            1,
            ConfigType.SYNC,
            "scope",
            JobConfig().apply {
              sync =
                JobSyncConfig().apply {
                  sourceDockerImageIsDefault = true
                  sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                  destinationDockerImageIsDefault = true
                  destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
                }
            },
            emptyList(),
            JobStatus.SUCCEEDED,
            0L,
            0L,
            0L,
          ),
        )
      } else {
        // Last call: return 2 jobs, 1 success and 1 failure
        listOf(
          Job(
            2,
            ConfigType.SYNC,
            "scope",
            JobConfig().apply {
              sync =
                JobSyncConfig().apply {
                  sourceDockerImageIsDefault = true
                  sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                  destinationDockerImageIsDefault = true
                  destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
                }
            },
            emptyList(),
            JobStatus.SUCCEEDED,
            0L,
            0L,
            0L,
          ),
          Job(
            3,
            ConfigType.SYNC,
            "scope",
            JobConfig().apply {
              sync =
                JobSyncConfig().apply {
                  sourceDockerImageIsDefault = true
                  sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
                  destinationDockerImageIsDefault = true
                  destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
                }
            },
            emptyList(),
            JobStatus.FAILED,
            0L,
            0L,
            0L,
          ),
        )
      }
    }

    val actorType = if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION

    val candidates =
      rolloutActorFinder.filterByJobStatus(
        createMockConnectorRollout(actorDefinitionId),
        CONFIG_SCOPE_MAP.values,
        MOCK_CONNECTION_SYNCS,
        actorType,
      )
    assertEquals(3, candidates.size)
    verify { jobService.listJobs(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `getIdFromConnection filters connections using sourceId when ActorType is SOURCE`() {
    val candidates =
      listOf(
        mapOf(
          ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
          ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
        ),
      ).flatMap { it.values }

    val connections =
      listOf(
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION
        },
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION
        },
      )

    val result = rolloutActorFinder.filterByConnectionActorId(candidates, connections, ActorType.SOURCE)

    assertEquals(2, result.size)
    assertEquals(ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE, result[0].sourceId)
    assertEquals(ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE, result[1].sourceId)
  }

  @Test
  fun `getIdFromConnection filters connections using destinationId when ActorType is DESTINATION`() {
    val candidates =
      listOf(
        mapOf(
          ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
          ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
        ),
      ).flatMap { it.values }

    val connections =
      listOf(
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION
        },
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION
        },
      )

    val result = rolloutActorFinder.filterByConnectionActorId(candidates, connections, ActorType.DESTINATION)

    assertEquals(2, result.size)
    assertEquals(ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION, result[0].destinationId)
    assertEquals(ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION, result[1].destinationId)
  }

  @Test
  fun `getIdFromConnection returns empty list if no matches are found`() {
    val candidates =
      listOf(
        mapOf(
          ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_DESTINATION,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
          ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION to
            ConfigScopeMapWithId(
              id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
              scopeMap =
                mapOf(
                  ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_DESTINATION,
                  ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                  ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
                ),
            ),
        ),
      ).flatMap { it.values }

    val connections =
      listOf(
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_2_WORKSPACE_1_ACTOR_ID_DESTINATION
        },
        StandardSync().apply {
          sourceId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID_SOURCE
          destinationId = ORGANIZATION_2_WORKSPACE_2_ACTOR_ID_DESTINATION
        },
      )

    val result = rolloutActorFinder.filterByConnectionActorId(candidates, connections, ActorType.DESTINATION)

    assertEquals(0, result.size)
  }

  @Test
  fun `test getUniqueActorIds with fewer actors than nActorsToPin`() {
    val sortedConnections =
      listOf(
        StandardSync().apply {
          sourceId = UUID.randomUUID()
          destinationId = UUID.randomUUID()
        },
        StandardSync().apply {
          sourceId = UUID.randomUUID()
          destinationId = UUID.randomUUID()
        },
      )

    val nActorsToPin = 5 // More than the available unique actor IDs
    val actorType = ActorType.SOURCE

    val result = rolloutActorFinder.getUniqueActorIds(sortedConnections, nActorsToPin, actorType)

    assertEquals(2, result.size, "Should return only available unique actor IDs without throwing an error.")
  }

  @Test
  fun `test getUniqueActorIds maintains order`() {
    val sourceId1 = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()
    val destinationId1 = UUID.randomUUID()
    val destinationId2 = UUID.randomUUID()

    val sortedConnections =
      listOf(
        StandardSync().apply {
          sourceId = sourceId1
          destinationId = destinationId1
        },
        StandardSync().apply {
          sourceId = sourceId2
          destinationId = destinationId2
        },
      )

    val nActorsToPin = 2
    val actorType = ActorType.SOURCE

    val result = rolloutActorFinder.getUniqueActorIds(sortedConnections, nActorsToPin, actorType)

    assertEquals(listOf(sourceId1, sourceId2), result, "The unique actor IDs should maintain the sort order from the input list.")
  }

  @ParameterizedTest
  @CsvSource("1", "2", "3")
  fun `test getUniqueActorIds with varying nActorsToPin`(nActorsToPin: Int) {
    val sourceId1 = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()
    val destinationId1 = UUID.randomUUID()
    val destinationId2 = UUID.randomUUID()

    val sortedConnections =
      listOf(
        StandardSync().apply {
          sourceId = sourceId1
          destinationId = destinationId1
        },
        StandardSync().apply {
          sourceId = sourceId2
          destinationId = destinationId2
        },
      )

    val actorType = ActorType.SOURCE
    val result = rolloutActorFinder.getUniqueActorIds(sortedConnections, nActorsToPin, actorType)

    val expectedSize = if (nActorsToPin > 2) 2 else nActorsToPin
    assertEquals(expectedSize, result.size, "The result should contain the minimum between the available actors and nActorsToPin.")
  }
}
