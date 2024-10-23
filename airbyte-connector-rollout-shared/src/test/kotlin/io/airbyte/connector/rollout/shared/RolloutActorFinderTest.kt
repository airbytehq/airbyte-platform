package io.airbyte.connector.rollout.shared

import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.JobStatus
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.Schedule
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.persistence.job.DefaultJobPersistence
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
import java.util.UUID

class RolloutActorFinderTest {
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val connectionService = mockk<ConnectionService>()
  private val jobPersistence = mockk<DefaultJobPersistence>()
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val rolloutActorFinder =
    RolloutActorFinder(
      actorDefinitionVersionUpdater,
      connectionService,
      jobPersistence,
      scopedConfigurationService,
      sourceService,
      destinationService,
    )

  companion object {
    private const val TARGET_PERCENTAGE = 50
    private val SOURCE_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val DESTINATION_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val RELEASE_CANDIDATE_VERSION_ID = UUID.randomUUID()

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

    val JOB_STATUS_SUMMARIES: List<JobStatusSummary> =
      MOCK_CONNECTION_SYNCS.mapIndexed { index, connection ->
        JobStatusSummary(
          connection.connectionId,
          connection.createdAt ?: 0L,
          JobStatus.SUCCEEDED,
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
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobPersistence.getLastSyncJobForConnections(any()) } returns JOB_STATUS_SUMMARIES

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
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any())
      jobPersistence.getLastSyncJobForConnections(any())
    }

    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      assertEquals(SOURCE_ACTOR_IDS.toSet().size * TARGET_PERCENTAGE / 100, actorSelectionInfo.actorIdsToPin.size)
    } else {
      assertEquals(DESTINATION_ACTOR_IDS.toSet().size * TARGET_PERCENTAGE / 100, actorSelectionInfo.actorIdsToPin.size)
    }
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
    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any()) } returns MOCK_CONNECTION_SYNCS
    every { jobPersistence.getLastSyncJobForConnections(any()) } returns JOB_STATUS_SUMMARIES

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
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any())
      jobPersistence.getLastSyncJobForConnections(any())
    }

    if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) {
      assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    } else {
      assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    }
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
  fun `test filterByTier is a no-op`(actorDefinitionId: UUID) {
    // This is not currently being used
    assertEquals(CONFIG_SCOPE_MAP.values, rolloutActorFinder.filterByTier(CONFIG_SCOPE_MAP.values))
  }

  @ParameterizedTest
  @MethodSource("actorDefinitionIds")
  fun `test getNPinnedToReleaseCandidate no actors pinned`(actorDefinitionId: UUID) {
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns emptyList()

    assertEquals(
      0,
      rolloutActorFinder.getNPinnedToReleaseCandidate(
        createMockConnectorRollout(actorDefinitionId),
      ),
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
          originType = ConfigOriginType.RELEASE_CANDIDATE
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.RELEASE_CANDIDATE
        },
      )

    assertEquals(2, rolloutActorFinder.getNPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)))

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
          originType = ConfigOriginType.RELEASE_CANDIDATE
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.RELEASE_CANDIDATE
        },
      )

    assertEquals(0, rolloutActorFinder.getNPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)))

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

    every { connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any()) } returns MOCK_CONNECTION_SYNCS

    val sortedConnectionSyncs =
      rolloutActorFinder.getSortedActorDefinitionConnections(
        CONFIG_SCOPE_MAP.values,
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
      connectionService.listConnectionsByActorDefinitionIdAndType(any(), any(), any())
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
    val jobStatusSummaries: List<JobStatusSummary> =
      MOCK_CONNECTION_SYNCS.mapIndexed { index, connection ->
        JobStatusSummary(
          connection.connectionId,
          connection.createdAt ?: 0L,
          if (index >= MOCK_CONNECTION_SYNCS.size - 2) JobStatus.FAILED else JobStatus.SUCCEEDED,
        )
      }
    every { jobPersistence.getLastSyncJobForConnections(any()) } returns jobStatusSummaries

    val actorType = if (actorDefinitionId == SOURCE_ACTOR_DEFINITION_ID) ActorType.SOURCE else ActorType.DESTINATION

    val candidates =
      rolloutActorFinder.filterByJobStatus(
        CONFIG_SCOPE_MAP.values,
        MOCK_CONNECTION_SYNCS,
        actorType,
      )
    candidates.forEach { candidate ->
      val connection =
        MOCK_CONNECTION_SYNCS.find { sync ->
          when (actorType) {
            ActorType.SOURCE -> sync.sourceId == candidate.id
            ActorType.DESTINATION -> sync.destinationId == candidate.id
          }
        }
      assertNotNull(connection, "Connection should not be null for candidate with actorId: ${candidate.id}")
      val jobStatusSummary = jobStatusSummaries.find { it.connectionId == connection!!.connectionId }
      assertEquals(JobStatus.SUCCEEDED, jobStatusSummary?.status, "The candidate list should only contain connections with successful jobs.")
    }

    verify { jobPersistence.getLastSyncJobForConnections(any()) }
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
