/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutState
import io.airbyte.api.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.api.model.generated.ConnectorRolloutTierFilter
import io.airbyte.api.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AttributeName
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFilters
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.Operator
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.client.ConnectorRolloutClient
import io.airbyte.connector.rollout.shared.RolloutActorFinder
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.NullSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class ConnectorRolloutHandlerManualTest {
  private val connectorRolloutService = mockk<ConnectorRolloutService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val userPersistence = mockk<UserPersistence>()
  private val connectorRolloutClient = mockk<ConnectorRolloutClient>()
  private val rolloutActorFinder = mockk<RolloutActorFinder>()
  private val connectorRolloutHelper =
    ConnectorRolloutHelper(
      connectorRolloutService,
      actorDefinitionService,
      userPersistence,
      rolloutActorFinder,
    )
  private val connectorRolloutHandler =
    ConnectorRolloutHandlerManual(
      1,
      1,
      1,
      connectorRolloutService,
      actorDefinitionService,
      actorDefinitionVersionUpdater,
      connectorRolloutClient,
      connectorRolloutHelper,
    )

  companion object {
    const val DOCKER_REPOSITORY = "airbyte/source-faker"
    const val DOCKER_IMAGE_TAG = "0.1"
    val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    val RELEASE_CANDIDATE_VERSION_ID: UUID = UUID.randomUUID()
    val UPDATED_BY = UUID.randomUUID()
    val DEFAULT_FILTERS =
      ConnectorRolloutFilters(
        customerTierFilters =
          listOf(
            CustomerTierFilter(name = AttributeName.TIER, operator = Operator.IN, value = listOf(CustomerTier.TIER_2)),
          ),
      )

    @JvmStatic
    fun validFinalizeStates() = listOf(ConnectorEnumRolloutState.FAILED_ROLLED_BACK, ConnectorEnumRolloutState.SUCCEEDED)

    @JvmStatic
    fun workflowStartedInProgress() =
      listOf(ConnectorEnumRolloutState.WORKFLOW_STARTED, ConnectorEnumRolloutState.IN_PROGRESS, ConnectorEnumRolloutState.PAUSED)
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test listConnectorRollouts`() {
    val mockActorDefinitionVersion = createMockActorDefinitionVersion()

    val expectedRollouts =
      listOf(
        createMockConnectorRollout(UUID.randomUUID(), ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID),
        createMockConnectorRollout(UUID.randomUUID(), ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID),
      )

    every {
      actorDefinitionService.getActorDefinitionVersion(
        ACTOR_DEFINITION_ID,
        DOCKER_IMAGE_TAG,
      )
    } returns Optional.of(mockActorDefinitionVersion)
    every { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID) } returns expectedRollouts
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns mockActorDefinitionVersion

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)

    assertEquals(expectedRollouts.map { connectorRolloutHelper.buildConnectorRolloutRead(it, false) }, rolloutReads)

    verify {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      actorDefinitionService.getActorDefinitionVersion(any())
      connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID)
    }
  }

  @Test
  fun `test listConnectorRolloutsByActorDefinitionVersion`() {
    val expectedRollouts =
      listOf(
        createMockConnectorRollout(UUID.randomUUID(), ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID),
        createMockConnectorRollout(UUID.randomUUID(), ACTOR_DEFINITION_ID, UUID.randomUUID()),
      )

    every { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID) } returns expectedRollouts
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(ACTOR_DEFINITION_ID)

    assertEquals(expectedRollouts.map { connectorRolloutHelper.buildConnectorRolloutRead(it, false) }, rolloutReads)

    verify {
      connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID)
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test listAllConnectorRollouts`() {
    val mockActorDefinitionVersion = createMockActorDefinitionVersion()

    val expectedRollouts =
      listOf(
        createMockConnectorRollout(UUID.randomUUID(), ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID),
        createMockConnectorRollout(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()),
      )

    every {
      actorDefinitionService.getActorDefinitionVersion(any(), any())
    } returns Optional.of(mockActorDefinitionVersion)
    every { connectorRolloutService.listConnectorRollouts() } returns expectedRollouts
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts()

    assertEquals(expectedRollouts.map { connectorRolloutHelper.buildConnectorRolloutRead(it, false) }, rolloutReads)

    verify {
      connectorRolloutService.listConnectorRollouts()
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test listConnectorRolloutsNoActorDefinitionVersion`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) } returns Optional.empty()

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(actorDefinitionId, DOCKER_IMAGE_TAG)

    assertEquals(emptyList<ConnectorRolloutRead>(), rolloutReads)

    verify {
      actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG)
    }
    verify(exactly = 0) { connectorRolloutService.listConnectorRollouts(actorDefinitionId, releaseCandidateVersionId) }
  }

  @Test
  fun `test manualStartConnectorRollout with no filters`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutWorkflowStart =
      ConnectorRolloutManualStartRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        updatedBy = UPDATED_BY
        rolloutStrategy = ConnectorRolloutStrategy.MANUAL
        migratePins = false
      }
    val connectorRollout = createMockConnectorRollout(rolloutId, rolloutStrategy = null)

    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    } returns Optional.of(createMockActorDefinitionVersion())
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(createMockActorDefinitionVersion())
    val rolloutSlot = slot<ConnectorRollout>()
    every { connectorRolloutService.writeConnectorRollout(capture(rolloutSlot)) } returns connectorRollout
    every { userPersistence.getUser(any()) } returns
      Optional.of(
        User().apply {
          userId = UUID.randomUUID()
          email = ""
        },
      )

    val result = connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutWorkflowStart)

    assertEquals(connectorRollout.id, result.id)
    assertEquals(connectorRollout.rolloutStrategy, ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRollout.updatedBy, UPDATED_BY)
    val capturedRollout = rolloutSlot.captured
    assertEquals(DEFAULT_FILTERS, capturedRollout.filters)

    verifyAll {
      connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      connectorRolloutService.listConnectorRollouts(any(), any())
      actorDefinitionService.getActorDefinitionVersion(any())
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
      connectorRolloutService.writeConnectorRollout(any())
      userPersistence.getUser(any())
    }
  }

  @Test
  fun `test manualStartConnectorRollout with filters`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutWorkflowStart =
      ConnectorRolloutManualStartRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        updatedBy = UPDATED_BY
        rolloutStrategy = ConnectorRolloutStrategy.MANUAL
        migratePins = false
        filters =
          io.airbyte.api.model.generated
            .ConnectorRolloutFilters()
            .tierFilter(
              ConnectorRolloutTierFilter().tier(
                io.airbyte.api.model.generated.CustomerTier.TIER_1,
              ),
            )
      }
    val connectorRollout = createMockConnectorRollout(rolloutId, rolloutStrategy = null)

    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    } returns Optional.of(createMockActorDefinitionVersion())
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(createMockActorDefinitionVersion())
    val rolloutSlot = slot<ConnectorRollout>()
    every { connectorRolloutService.writeConnectorRollout(capture(rolloutSlot)) } returns connectorRollout
    every { userPersistence.getUser(any()) } returns
      Optional.of(
        User().apply {
          userId = UUID.randomUUID()
          email = ""
        },
      )

    val result = connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutWorkflowStart)

    assertEquals(connectorRollout.id, result.id)
    assertEquals(connectorRollout.rolloutStrategy, ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRollout.updatedBy, UPDATED_BY)
    val capturedRollout = rolloutSlot.captured
    assertEquals(
      ConnectorRolloutFilters(
        customerTierFilters =
          listOf(
            CustomerTierFilter(name = AttributeName.TIER, operator = Operator.IN, value = listOf(CustomerTier.TIER_1)),
          ),
      ),
      capturedRollout.filters,
    )

    verifyAll {
      connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      connectorRolloutService.listConnectorRollouts(any(), any())
      actorDefinitionService.getActorDefinitionVersion(any())
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
      connectorRolloutService.writeConnectorRollout(any())
      userPersistence.getUser(any())
    }
  }

  @Test
  fun `test manualStartConnectorRollout all tiers`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutWorkflowStart =
      ConnectorRolloutManualStartRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        updatedBy = UPDATED_BY
        rolloutStrategy = ConnectorRolloutStrategy.MANUAL
        migratePins = false
        filters =
          io.airbyte.api.model.generated
            .ConnectorRolloutFilters()
            .tierFilter(
              ConnectorRolloutTierFilter().tier(
                io.airbyte.api.model.generated.CustomerTier.ALL,
              ),
            )
      }
    val connectorRollout = createMockConnectorRollout(rolloutId, rolloutStrategy = null)

    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    } returns Optional.of(createMockActorDefinitionVersion())
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(createMockActorDefinitionVersion())
    val rolloutSlot = slot<ConnectorRollout>()
    every { connectorRolloutService.writeConnectorRollout(capture(rolloutSlot)) } returns connectorRollout
    every { userPersistence.getUser(any()) } returns
      Optional.of(
        User().apply {
          userId = UUID.randomUUID()
          email = ""
        },
      )

    val result = connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutWorkflowStart)

    assertEquals(connectorRollout.id, result.id)
    assertEquals(connectorRollout.rolloutStrategy, ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRollout.updatedBy, UPDATED_BY)
    val capturedRollout = rolloutSlot.captured
    assertEquals(
      ConnectorRolloutFilters(
        customerTierFilters = emptyList(),
      ),
      capturedRollout.filters,
    )

    verifyAll {
      connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      connectorRolloutService.listConnectorRollouts(any(), any())
      actorDefinitionService.getActorDefinitionVersion(any())
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
      connectorRolloutService.writeConnectorRollout(any())
      userPersistence.getUser(any())
    }
  }

  @Test
  fun `test getRolloutStrategyForManualStart`() {
    assertEquals(connectorRolloutHandler.getRolloutStrategyForManualStart(null), ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRolloutHandler.getRolloutStrategyForManualStart(ConnectorRolloutStrategy.MANUAL), ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRolloutHandler.getRolloutStrategyForManualStart(ConnectorRolloutStrategy.AUTOMATED), ConnectorEnumRolloutStrategy.AUTOMATED)
  }

  @Test
  fun `test getRolloutStrategyForManualUpdate`() {
    assertEquals(connectorRolloutHandler.getRolloutStrategyForManualUpdate(null), ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(connectorRolloutHandler.getRolloutStrategyForManualUpdate(ConnectorEnumRolloutStrategy.MANUAL), ConnectorEnumRolloutStrategy.MANUAL)
    assertEquals(
      connectorRolloutHandler.getRolloutStrategyForManualUpdate(ConnectorEnumRolloutStrategy.AUTOMATED),
      ConnectorEnumRolloutStrategy.OVERRIDDEN,
    )
  }

  @ParameterizedTest
  @MethodSource("workflowStartedInProgress")
  fun `test manualDoConnectorRolloutWorkflowUpdate workflow already in progress`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val actorIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val connectorRolloutWorkflowUpdate =
      ConnectorRolloutManualRolloutRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        id = rolloutId
        this.actorIds = actorIds
      }
    val connectorRollout = createMockConnectorRollout(rolloutId)
    // Rollout has been started
    connectorRollout.apply { this.state = state }

    every { connectorRolloutClient.doRollout(any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    connectorRolloutHandler.manualDoConnectorRollout(connectorRolloutWorkflowUpdate)

    verifyAll {
      connectorRolloutClient.doRollout(any(), any(), any(), any(), any(), any(), any(), any())
      connectorRolloutService.getConnectorRollout(rolloutId)
    }

    // Verify that startWorkflow() was not called because the rollout is already in progress
    verify(exactly = 0) { connectorRolloutClient.startRolloutWorkflow(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @MethodSource("validFinalizeStates")
  fun `test manualFinalizeConnectorRollout`(initialState: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val rollout = createMockConnectorRollout(rolloutId)
    rollout.apply { state = initialState }
    val state = ConnectorRolloutStateTerminal.SUCCEEDED
    val connectorRolloutFinalizeWorkflowUpdate =
      ConnectorRolloutManualFinalizeRequestBody().apply {
        dockerRepository = "airbyte/source-faker"
        dockerImageTag = "0.1"
        actorDefinitionId = UUID.randomUUID()
        id = rolloutId
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns rollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { connectorRolloutClient.finalizeRollout(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    connectorRolloutHandler.manualFinalizeConnectorRollout(connectorRolloutFinalizeWorkflowUpdate)

    verifyAll {
      connectorRolloutService.getConnectorRollout(any())
      actorDefinitionService.getActorDefinitionVersion(any())
      connectorRolloutClient.finalizeRollout(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    }
  }

  @Test
  fun `test manualCancelConnectorRollout without pin retention`() {
    val rolloutId = UUID.randomUUID()
    val rollout = createMockConnectorRollout(rolloutId)
    val state = ConnectorRolloutStateTerminal.CANCELED
    val connectorRolloutFinalizeWorkflowUpdate =
      ConnectorRolloutManualFinalizeRequestBody().apply {
        dockerRepository = "airbyte/source-faker"
        dockerImageTag = "0.1"
        actorDefinitionId = UUID.randomUUID()
        id = rolloutId
        this.state = state
        retainPinsOnCancellation = false
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns rollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) } just Runs
    every { connectorRolloutClient.cancelRollout(any(), any(), any(), any(), any(), any()) } returns Unit
    every { connectorRolloutService.writeConnectorRollout(any()) } returns mockk()

    connectorRolloutHandler.manualFinalizeConnectorRollout(connectorRolloutFinalizeWorkflowUpdate)

    verify(exactly = 0) {
      connectorRolloutClient.finalizeRollout(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    }
    verifyAll {
      connectorRolloutService.getConnectorRollout(any())
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any())
      connectorRolloutClient.cancelRollout(any(), any(), any(), any(), any(), any())
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @Test
  fun `test manuaCancelConnectorRollout with pin retention`() {
    val rolloutId = UUID.randomUUID()
    val rollout = createMockConnectorRollout(rolloutId)
    val state = ConnectorRolloutStateTerminal.CANCELED
    val connectorRolloutFinalizeWorkflowUpdate =
      ConnectorRolloutManualFinalizeRequestBody().apply {
        dockerRepository = "airbyte/source-faker"
        dockerImageTag = "0.1"
        actorDefinitionId = UUID.randomUUID()
        id = rolloutId
        this.state = state
        retainPinsOnCancellation = true
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns rollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { connectorRolloutClient.cancelRollout(any(), any(), any(), any(), any(), any()) } returns Unit
    every { connectorRolloutService.writeConnectorRollout(any()) } returns mockk()

    connectorRolloutHandler.manualFinalizeConnectorRollout(connectorRolloutFinalizeWorkflowUpdate)

    verify(exactly = 0) { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) }
    verifyAll {
      connectorRolloutService.getConnectorRollout(any())
      connectorRolloutClient.cancelRollout(any(), any(), any(), any(), any(), any())
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("workflowStartedInProgress")
  fun `test manualPauseConnectorRollout`(rolloutState: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutWorkflowUpdate =
      ConnectorRolloutUpdateStateRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        id = rolloutId
        state = ConnectorRolloutState.PAUSED
        pausedReason = "test"
      }
    val connectorRollout = createMockConnectorRollout(rolloutId)
    // Rollout has been started
    connectorRollout.apply { this.state = state }

    every { connectorRolloutClient.pauseRollout(any(), any(), any(), any(), any(), any(), any()) } returns
      ConnectorRolloutOutput(state = ConnectorEnumRolloutState.PAUSED)
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    val result = connectorRolloutHandler.manualPauseConnectorRollout(connectorRolloutWorkflowUpdate)

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      connectorRolloutClient.pauseRollout(any(), any(), any(), any(), any(), any(), any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput updates rollout when already exists in INITIALIZED state`() {
    val rolloutId = UUID.randomUUID()
    val dockerImageTag = "0.1"
    val actorDefinitionId = UUID.randomUUID()
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = ConnectorEnumRolloutState.INITIALIZED
      }

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) } returns Optional.of(actorDefinitionVersion)
    every { connectorRolloutService.listConnectorRollouts(actorDefinitionId, actorDefinitionVersion.versionId) } returns listOf(connectorRollout)
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(createMockActorDefinitionVersion())
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout

    val result =
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        DOCKER_REPOSITORY,
        actorDefinitionId,
        dockerImageTag,
        UPDATED_BY,
        ConnectorRolloutStrategy.MANUAL,
        null,
        null,
        null,
      )

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
      connectorRolloutService.listConnectorRollouts(actorDefinitionId, actorDefinitionVersion.versionId)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput throws when initial version is not found`() {
    val rolloutId = UUID.randomUUID()
    val dockerImageTag = "0.1"
    val actorDefinitionId = UUID.randomUUID()
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = ConnectorEnumRolloutState.INITIALIZED
      }

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) } returns Optional.of(actorDefinitionVersion)
    every { connectorRolloutService.listConnectorRollouts(actorDefinitionId, actorDefinitionVersion.versionId) } returns listOf(connectorRollout)
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        DOCKER_REPOSITORY,
        actorDefinitionId,
        dockerImageTag,
        UPDATED_BY,
        ConnectorRolloutStrategy.MANUAL,
        null,
        null,
        null,
      )
    }
  }

  @Test
  fun `test validateAutomatedInitialRolloutPercent`() {
    val validPercents = listOf(1, 50, 100)

    validPercents.forEach { pct ->
      assertDoesNotThrow {
        connectorRolloutHandler.validateAutomatedInitialRolloutPercent(pct)
      }
    }

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.validateAutomatedInitialRolloutPercent(null)
    }

    val invalidPercents = listOf(0, -1, 101)

    invalidPercents.forEach { pct ->
      assertThrows<ConnectorRolloutInvalidRequestProblem> {
        connectorRolloutHandler.validateAutomatedInitialRolloutPercent(pct)
      }
    }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput creates new connector rollout if not found`() {
    val rolloutId = UUID.randomUUID()
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = ConnectorEnumRolloutState.INITIALIZED
      }

    every { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, actorDefinitionVersion.versionId) } returns emptyList()
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.of(actorDefinitionVersion)
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID) } returns Optional.of(actorDefinitionVersion)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout

    connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
      DOCKER_REPOSITORY,
      ACTOR_DEFINITION_ID,
      DOCKER_IMAGE_TAG,
      UPDATED_BY,
      ConnectorRolloutStrategy.MANUAL,
      null,
      null,
      null,
    )

    verifyAll {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, actorDefinitionVersion.versionId)
      connectorRolloutService.writeConnectorRollout(any())
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID)
    }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput throws ConnectorRolloutInvalidRequestProblem when actor definition version not found`() {
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.empty()

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        DOCKER_REPOSITORY,
        ACTOR_DEFINITION_ID,
        DOCKER_IMAGE_TAG,
        UPDATED_BY,
        ConnectorRolloutStrategy.MANUAL,
        null,
        null,
        null,
      )
    }

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput throws ConnectorRolloutInvalidRequestProblem when docker repository doesn't match`() {
    val dockerRepository = "wrong-repo"
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.of(actorDefinitionVersion)

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        dockerRepository,
        ACTOR_DEFINITION_ID,
        DOCKER_IMAGE_TAG,
        UPDATED_BY,
        ConnectorRolloutStrategy.MANUAL,
        null,
        null,
        null,
      )
    }

    verifyAll {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(ints = [0, -1, 101])
  fun `test getOrCreateAndValidateManualStartInput throws ConnectorRolloutInvalidRequestProblem when automated and initial rollout pct is invalid`(
    initialRolloutPct: Int?,
  ) {
    val rolloutId = UUID.randomUUID()
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = ConnectorEnumRolloutState.INITIALIZED
      }

    every { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, actorDefinitionVersion.versionId) } returns emptyList()
    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.of(actorDefinitionVersion)
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID) } returns Optional.of(actorDefinitionVersion)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        DOCKER_REPOSITORY,
        ACTOR_DEFINITION_ID,
        DOCKER_IMAGE_TAG,
        UPDATED_BY,
        ConnectorRolloutStrategy.AUTOMATED,
        initialRolloutPct,
        null,
        null,
      )
    }
  }

  @Test
  fun `returns tag for single tier`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())
    rollout.filters =
      ConnectorRolloutFilters(
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              operator = Operator.IN,
              value = listOf(CustomerTier.TIER_1),
            ),
          ),
      )

    val tag = connectorRolloutHandler.createTagFromFilters(rollout.filters)
    assertEquals("TIER_1", tag)
  }

  @Test
  fun `returns tag for multiple tiers sorted`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())
    rollout.filters =
      ConnectorRolloutFilters(
        customerTierFilters =
          listOf(
            CustomerTierFilter(
              operator = Operator.IN,
              value = listOf(CustomerTier.TIER_2, CustomerTier.TIER_0),
            ),
          ),
      )

    val tag = connectorRolloutHandler.createTagFromFilters(rollout.filters)
    assertEquals("TIER_0-TIER_2", tag)
  }

  @Test
  fun `returns null when tier filter is empty`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())
    rollout.filters =
      ConnectorRolloutFilters(
        customerTierFilters = emptyList(),
      )

    val tag = connectorRolloutHandler.createTagFromFilters(rollout.filters)
    assertNull(tag)
  }

  @Test
  fun `returns null when filters is null`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())

    val tag = connectorRolloutHandler.createTagFromFilters(rollout.filters)
    assertNull(tag)
  }

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = ACTOR_DEFINITION_ID,
    releaseCandidateVersionId: UUID = RELEASE_CANDIDATE_VERSION_ID,
    rolloutStrategy: ConnectorEnumRolloutStrategy? = ConnectorEnumRolloutStrategy.MANUAL,
  ): ConnectorRollout =
    ConnectorRollout(
      id = id,
      actorDefinitionId = actorDefinitionId,
      releaseCandidateVersionId = releaseCandidateVersionId,
      initialVersionId = UUID.randomUUID(),
      state = ConnectorEnumRolloutState.INITIALIZED,
      initialRolloutPct = 10,
      finalTargetRolloutPct = 100,
      hasBreakingChanges = false,
      rolloutStrategy = rolloutStrategy,
      maxStepWaitTimeMins = 60,
      createdAt = OffsetDateTime.now().toEpochSecond(),
      updatedAt = OffsetDateTime.now().toEpochSecond(),
      expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond(),
      tag = null,
    )

  private fun createMockActorDefinitionVersion(): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withVersionId(RELEASE_CANDIDATE_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
}
