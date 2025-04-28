/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutMaximumRolloutPercentageReachedProblem
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutNotEnoughActorsProblem
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.shared.ActorSelectionInfo
import io.airbyte.connector.rollout.shared.ActorSyncJobInfo
import io.airbyte.connector.rollout.shared.RolloutActorFinder
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.ScopedConfigurationService
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

internal class ConnectorRolloutHandlerTest {
  private val connectorRolloutService = mockk<ConnectorRolloutService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val userPersistence = mockk<UserPersistence>()
  private val rolloutActorFinder = mockk<RolloutActorFinder>()
  private val connectorRolloutHelper =
    ConnectorRolloutHelper(
      connectorRolloutService,
      actorDefinitionService,
      userPersistence,
      rolloutActorFinder,
    )
  private val connectorRolloutHandler =
    ConnectorRolloutHandler(
      connectorRolloutService,
      actorDefinitionService,
      actorDefinitionVersionUpdater,
      userPersistence,
      rolloutActorFinder,
      connectorRolloutHelper,
    )

  companion object {
    const val DOCKER_REPOSITORY = "airbyte/source-faker"
    const val DOCKER_IMAGE_TAG = "0.1"
    val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    val RELEASE_CANDIDATE_VERSION_ID: UUID = UUID.randomUUID()

    @JvmStatic
    fun validStartStates() = listOf(ConnectorEnumRolloutState.INITIALIZED)

    @JvmStatic
    fun invalidStartStates() = ConnectorEnumRolloutState.entries.filter { it != ConnectorEnumRolloutState.INITIALIZED }

    @JvmStatic
    fun validUpdateStates() =
      setOf(
        ConnectorEnumRolloutState.INITIALIZED,
        ConnectorEnumRolloutState.WORKFLOW_STARTED,
        ConnectorEnumRolloutState.IN_PROGRESS,
        ConnectorEnumRolloutState.PAUSED,
      )

    @JvmStatic
    fun invalidUpdateStates() =
      ConnectorEnumRolloutState.entries.filterNot { state ->
        validUpdateStates().map { it.name }.contains(state.name)
      }

    @JvmStatic
    fun validFinalizeStates() =
      ConnectorEnumRolloutState.entries.filterNot { state ->
        ConnectorRolloutFinalState.entries.map { it.name }.contains(state.name)
      }

    @JvmStatic
    fun invalidFinalizeStates() =
      ConnectorEnumRolloutState.entries.filterNot { state ->
        // If the rollout is already finalized, it can't be finalized again
        validFinalizeStates().map { it.name }.contains(state.name)
      }

    @JvmStatic
    fun provideConnectorRolloutStateTerminalNonCanceled(): List<ConnectorRolloutStateTerminal> =
      listOf(
        ConnectorRolloutStateTerminal.SUCCEEDED,
        ConnectorRolloutStateTerminal.FAILED_ROLLED_BACK,
      )
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test getConnectorRollout by id`() {
    val rolloutId = UUID.randomUUID()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns expectedRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(listOf(), 0, 0, 0, 0)
    every { rolloutActorFinder.getSyncInfoForPinnedActors(any()) } returns emptyMap()

    val rolloutRead = connectorRolloutHandler.getConnectorRollout(rolloutId)

    assertEquals(connectorRolloutHelper.buildConnectorRolloutRead(expectedRollout, true), rolloutRead)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionService.getActorDefinitionVersion(any())
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
      rolloutActorFinder.getSyncInfoForPinnedActors(any())
    }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with scoped configurations insertion`() {
    val rolloutId = UUID.randomUUID()
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val actorIds = listOf(UUID.randomUUID(), UUID.randomUUID())

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      ).apply {
        this.state = ConnectorEnumRolloutState.WORKFLOW_STARTED
      }

    val connectorRolloutRequestBody =
      createMockConnectorRolloutRequestBody(rolloutId, rolloutStrategy, actorIds, null).apply {
        this.actorIds = actorIds
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(emptyList(), 1, 1, 1, 1)

    val updatedRollout = connectorRolloutHandler.getAndRollOutConnectorRollout(connectorRolloutRequestBody)

    assertEquals(rolloutStrategy.toString(), updatedRollout.rolloutStrategy.toString())
    assertTrue(
      updatedRollout.updatedAt >= initialRollout.updatedAt,
      "updatedAt from doRollout should be more recent than updatedAt from initialRollout",
    )

    // Ensure other fields are not modified
    val excludedFields = listOf("rolloutPct", "rolloutStrategy", "updatedAt")

    ConnectorRollout::class
      .memberProperties
      .filter { it.name !in excludedFields }
      .forEach { property ->
        property.isAccessible = true
        val initialValue = property.get(initialRollout)
        val updatedValue = property.get(updatedRollout)

        assertEquals(initialValue, updatedValue, "Field ${property.name} should not be modified")
      }

    verifyAll {
      connectorRolloutService.getConnectorRollout(any())
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any())
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
    }
  }

  @Test
  fun `test finalizeConnectorRollout with scoped configurations deletion`() {
    val rolloutId = UUID.randomUUID()
    val state = ConnectorRolloutStateTerminal.SUCCEEDED
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val errorMsg = "error"
    val failedReason = "failure"
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      )

    val connectorRolloutFinalizeRequestBody =
      createMockConnectorRolloutFinalizeRequestBody(
        rolloutId = rolloutId,
        state = state,
        rolloutStrategy = rolloutStrategy,
        errorMsg = errorMsg,
        failedReason = failedReason,
        retainPinsOnCancellation = true,
      )
    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) } returns Unit

    val finalizedRollout = connectorRolloutHandler.getAndValidateFinalizeRequest(connectorRolloutFinalizeRequestBody)

    verify {
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any())
    }

    assertEquals(state.toString(), finalizedRollout.state.toString())
    assertEquals(rolloutStrategy.toString(), finalizedRollout.rolloutStrategy.toString())
    assertEquals(errorMsg, finalizedRollout.errorMsg)
    assertEquals(failedReason, finalizedRollout.failedReason)
    assertTrue(
      finalizedRollout.updatedAt >= initialRollout.updatedAt,
      "updatedAt from finalizedRollout should be more recent than updatedAt from initialRollout",
    )
  }

  @Test
  fun `test finalizeConnectorRollout with retainPinsOnCancellation and canceled`() {
    val rolloutId = UUID.randomUUID()
    val state = ConnectorRolloutStateTerminal.CANCELED
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val errorMsg = "error"
    val failedReason = "failure"
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      )

    val connectorRolloutFinalizeRequestBody =
      createMockConnectorRolloutFinalizeRequestBody(
        rolloutId = rolloutId,
        state = state,
        rolloutStrategy = rolloutStrategy,
        errorMsg = errorMsg,
        failedReason = failedReason,
        retainPinsOnCancellation = true,
      )
    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) } returns Unit

    val finalizedRollout = connectorRolloutHandler.getAndValidateFinalizeRequest(connectorRolloutFinalizeRequestBody)

    verify(exactly = 0) {
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any())
    }

    assertEquals(state.toString(), finalizedRollout.state.toString())
    assertEquals(rolloutStrategy.toString(), finalizedRollout.rolloutStrategy.toString())
    assertEquals(errorMsg, finalizedRollout.errorMsg)
    assertEquals(failedReason, finalizedRollout.failedReason)
    assertTrue(
      finalizedRollout.updatedAt >= initialRollout.updatedAt,
      "updatedAt from finalizedRollout should be more recent than updatedAt from initialRollout",
    )
  }

  @Test
  fun `test finalizeConnectorRollout without retainPinsOnCancellation and canceled`() {
    val rolloutId = UUID.randomUUID()
    val state = ConnectorRolloutStateTerminal.CANCELED
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val errorMsg = "error"
    val failedReason = "failure"
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      )

    val connectorRolloutFinalizeRequestBody =
      createMockConnectorRolloutFinalizeRequestBody(
        rolloutId = rolloutId,
        state = state,
        rolloutStrategy = rolloutStrategy,
        errorMsg = errorMsg,
        failedReason = failedReason,
        retainPinsOnCancellation = false,
      )
    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) } returns Unit

    val finalizedRollout = connectorRolloutHandler.getAndValidateFinalizeRequest(connectorRolloutFinalizeRequestBody)

    verify {
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any())
    }

    assertEquals(state.toString(), finalizedRollout.state.toString())
    assertEquals(rolloutStrategy.toString(), finalizedRollout.rolloutStrategy.toString())
    assertEquals(errorMsg, finalizedRollout.errorMsg)
    assertEquals(failedReason, finalizedRollout.failedReason)
    assertTrue(
      finalizedRollout.updatedAt >= initialRollout.updatedAt,
      "updatedAt from finalizedRollout should be more recent than updatedAt from initialRollout",
    )
  }

  @ParameterizedTest
  @MethodSource("provideConnectorRolloutStateTerminalNonCanceled")
  fun `test finalizeConnectorRollout with retainPinsOnCancellation and not canceled is ignored`(state: ConnectorRolloutStateTerminal) {
    val rolloutId = UUID.randomUUID()
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val errorMsg = "error"
    val failedReason = "failure"
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      )

    val connectorRolloutFinalizeRequestBody =
      createMockConnectorRolloutFinalizeRequestBody(
        rolloutId = rolloutId,
        state = state,
        rolloutStrategy = rolloutStrategy,
        errorMsg = errorMsg,
        failedReason = failedReason,
        retainPinsOnCancellation = true,
      )
    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any()) } returns Unit

    val finalizedRollout = connectorRolloutHandler.getAndValidateFinalizeRequest(connectorRolloutFinalizeRequestBody)

    verify {
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(any(), any())
    }

    assertEquals(state.toString(), finalizedRollout.state.toString())
    assertEquals(rolloutStrategy.toString(), finalizedRollout.rolloutStrategy.toString())
    assertEquals(errorMsg, finalizedRollout.errorMsg)
    assertEquals(failedReason, finalizedRollout.failedReason)
    assertTrue(
      finalizedRollout.updatedAt >= initialRollout.updatedAt,
      "updatedAt from finalizedRollout should be more recent than updatedAt from initialRollout",
    )
  }

  @Test
  fun `test validateRolloutActorDefinitionId with matching version`() {
    val mockActorDefinitionVersion = createMockActorDefinitionVersion()
    val dockerRepository = mockActorDefinitionVersion.dockerRepository
    val dockerImageTag = mockActorDefinitionVersion.dockerImageTag
    val actorDefinitionId = mockActorDefinitionVersion.actorDefinitionId

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) } returns Optional.of(mockActorDefinitionVersion)

    connectorRolloutHandler.validateRolloutActorDefinitionId(dockerRepository!!, dockerImageTag!!, actorDefinitionId!!)

    verify {
      actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
    }
  }

  @Test
  fun `test validateRolloutActorDefinitionId with mismatched repository throws exception`() {
    val mockActorDefinitionVersion = createMockActorDefinitionVersion()
    val dockerRepository = "wrong/repo"
    val actorDefinitionId = createMockActorDefinitionVersion().actorDefinitionId

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) } returns Optional.of(mockActorDefinitionVersion)

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.validateRolloutActorDefinitionId(dockerRepository, DOCKER_IMAGE_TAG, actorDefinitionId)
    }

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) }
  }

  @Test
  fun `test startConnectorRollout with pin migration`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId).apply { this.state = state }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { connectorRolloutService.listConnectorRollouts(any()) } returns emptyList()
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(listOf(), 0, 0, 0, 0)
    every { rolloutActorFinder.getSyncInfoForPinnedActors(any()) } returns emptyMap()
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { actorDefinitionVersionUpdater.migrateReleaseCandidatePins(any(), any(), any(), any()) } just Runs
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout

    connectorRolloutHandler.startConnectorRollout(
      ConnectorRolloutStartRequestBody()
        .id(rolloutId)
        .workflowRunId(UUID.randomUUID().toString())
        .rolloutStrategy(ConnectorRolloutStrategy.MANUAL)
        .migratePins(true),
    )

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionVersionUpdater.migrateReleaseCandidatePins(any(), any(), any(), any())
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
      rolloutActorFinder.getSyncInfoForPinnedActors(any())
      actorDefinitionService.getActorDefinitionVersion(any())
      connectorRolloutService.listConnectorRollouts(any())
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @Test
  fun `test startConnectorRollout without pin migration`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId).apply { this.state = state }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { connectorRolloutService.listConnectorRollouts(any()) } returns emptyList()
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(listOf(), 0, 0, 0, 0)
    every { rolloutActorFinder.getSyncInfoForPinnedActors(any()) } returns emptyMap()
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout

    connectorRolloutHandler.startConnectorRollout(
      ConnectorRolloutStartRequestBody()
        .id(rolloutId)
        .workflowRunId(UUID.randomUUID().toString())
        .rolloutStrategy(ConnectorRolloutStrategy.MANUAL)
        .migratePins(false),
    )

    verify(exactly = 0) {
      actorDefinitionVersionUpdater.migrateReleaseCandidatePins(any(), any(), any(), any())
      connectorRolloutService.listConnectorRollouts(any())
    }
    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
      rolloutActorFinder.getSyncInfoForPinnedActors(any())
      actorDefinitionService.getActorDefinitionVersion(any())
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("validStartStates")
  fun `test getAndValidateStartRequest with initialized state`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId).apply { this.state = state }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    val result =
      connectorRolloutHandler.getAndValidateStartRequest(
        ConnectorRolloutStartRequestBody()
          .id(rolloutId)
          .workflowRunId(UUID.randomUUID().toString())
          .rolloutStrategy(ConnectorRolloutStrategy.MANUAL),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
    }
  }

  @ParameterizedTest
  @MethodSource("invalidStartStates")
  fun `test getAndValidateStartRequest adds new information to existing rollout`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val runId = UUID.randomUUID().toString()
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val connectorRollout =
      createMockConnectorRollout(rolloutId, rolloutStrategy = ConnectorEnumRolloutStrategy.MANUAL).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns connectorRollout

    val result =
      connectorRolloutHandler.getAndValidateStartRequest(
        ConnectorRolloutStartRequestBody()
          .id(rolloutId)
          .workflowRunId(runId)
          .rolloutStrategy(rolloutStrategy),
      )

    assertEquals(connectorRollout.workflowRunId, result.workflowRunId)
    assertEquals(rolloutStrategy.toString(), result.rolloutStrategy.toString())
    verify {
      connectorRolloutService.getConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("validUpdateStates")
  fun `test getAndRollOutConnectorRollout with valid state`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(emptyList(), 1, 1, 1, 1)

    val result =
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, listOf(UUID.randomUUID()), null),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any())
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
    }
  }

  @ParameterizedTest
  @MethodSource("invalidUpdateStates")
  fun `test getAndRollOutConnectorRollout with invalid state throws exception`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, listOf(UUID.randomUUID()), null),
      )
    }

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
    }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with actorIds`() {
    val rolloutId = UUID.randomUUID()
    val actorIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        state = ConnectorEnumRolloutState.IN_PROGRESS
      }
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns ActorSelectionInfo(emptyList(), 1, 1, 1, 0)

    val result =
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, actorIds, null),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(actorIds.toSet(), ACTOR_DEFINITION_ID, any(), any())
    }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with targetPercentage`() {
    val rolloutId = UUID.randomUUID()
    val actorIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        state = ConnectorEnumRolloutState.IN_PROGRESS
      }
    val mockActorSelectionInfo = ActorSelectionInfo(actorIds, 2, 2, 2, 0)
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns mockActorSelectionInfo

    val result =
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, actorIds, 100),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(actorIds.toSet(), ACTOR_DEFINITION_ID, any(), any())
    }
    verify { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with actorIds and targetPercentage`() {
    val rolloutId = UUID.randomUUID()
    val inputActorIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val selectedActorIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        state = ConnectorEnumRolloutState.IN_PROGRESS
        currentTargetRolloutPct = 100
        finalTargetRolloutPct = 100
      }
    val mockActorSelectionInfo = ActorSelectionInfo(selectedActorIds, 3, 2, 2, 1)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns mockActorSelectionInfo

    val result =
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, inputActorIds, 100),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      rolloutActorFinder.getActorSelectionInfo(any(), any(), any())
    }
    verify(exactly = 1) {
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(inputActorIds.toSet(), any(), any(), any())
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(selectedActorIds.toSet(), any(), any(), any())
    }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with no actorIds or targetPercentage throws`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        state = ConnectorEnumRolloutState.IN_PROGRESS
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, null, null),
      )
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test getAndRollOutConnectorRollout with too few eligible actorIds found throws`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        state = ConnectorEnumRolloutState.IN_PROGRESS
        currentTargetRolloutPct = 100
      }
    val mockActorSelectionInfo = ActorSelectionInfo(listOf(), 3, 2, 2, 1)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { rolloutActorFinder.getActorSelectionInfo(any(), any(), any()) } returns mockActorSelectionInfo

    assertThrows<ConnectorRolloutNotEnoughActorsProblem> {
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL, null, 50),
      )
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test validateCanPin for manual rollouts does not enforce finalTargetRolloutPct`() {
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID())
    connectorRollout.finalTargetRolloutPct = 50

    assertEquals(
      100,
      connectorRolloutHandler.getValidPercentageToPin(connectorRollout, 100, rolloutStrategy = ConnectorRolloutStrategy.MANUAL, 0),
    )
  }

  @Test
  fun `test validateCanPin for automated rollouts when current percentage is less than max`() {
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID())
    connectorRollout.finalTargetRolloutPct = 100

    assertEquals(
      75,
      connectorRolloutHandler.getValidPercentageToPin(connectorRollout, 75, rolloutStrategy = ConnectorRolloutStrategy.AUTOMATED, 50),
    )
    assertEquals(
      100,
      connectorRolloutHandler.getValidPercentageToPin(connectorRollout, 100, rolloutStrategy = ConnectorRolloutStrategy.AUTOMATED, 50),
    )
  }

  @Test
  fun `test validateCanPin for automated rollouts throws when current percentage equals or exceeds max`() {
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID())
    connectorRollout.finalTargetRolloutPct = 50
    connectorRollout.currentTargetRolloutPct = 50

    assertThrows<ConnectorRolloutMaximumRolloutPercentageReachedProblem> {
      connectorRolloutHandler.getValidPercentageToPin(connectorRollout, 50, rolloutStrategy = ConnectorRolloutStrategy.AUTOMATED, 50)
    }
    assertThrows<ConnectorRolloutMaximumRolloutPercentageReachedProblem> {
      connectorRolloutHandler.getValidPercentageToPin(connectorRollout, 100, rolloutStrategy = ConnectorRolloutStrategy.AUTOMATED, 50)
    }
  }

  @ParameterizedTest
  @MethodSource("validFinalizeStates")
  fun `test getAndValidateFinalizeRequest with valid state`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID) } returns Unit

    val result =
      connectorRolloutHandler.getAndValidateFinalizeRequest(
        createMockConnectorRolloutFinalizeRequestBody(
          rolloutId,
          ConnectorRolloutStateTerminal.SUCCEEDED,
          ConnectorRolloutStrategy.MANUAL,
          "No error",
          "No failure",
          true,
        ),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID)
    }
  }

  @ParameterizedTest
  @MethodSource("invalidFinalizeStates")
  fun `test getAndValidateFinalizeRequest with invalid state throws exception`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getAndValidateFinalizeRequest(
        createMockConnectorRolloutFinalizeRequestBody(
          rolloutId,
          ConnectorRolloutStateTerminal.SUCCEEDED,
          ConnectorRolloutStrategy.MANUAL,
          "No error",
          "No failure",
          true,
        ),
      )
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @ParameterizedTest
  @MethodSource("validFinalizeStates")
  fun `test getAndValidateUpdateStateRequest with valid state`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    val result =
      connectorRolloutHandler.getAndValidateUpdateStateRequest(rolloutId, ConnectorEnumRolloutState.FINALIZING, null, null, null)

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
    }
  }

  @ParameterizedTest
  @MethodSource("invalidFinalizeStates")
  fun `test getAndValidateUpdateStateRequest with invalid state throws exception`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getAndValidateUpdateStateRequest(rolloutId, ConnectorEnumRolloutState.SUCCEEDED, null, null, null)
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test getActorSyncInfo`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId)
    val actorId = UUID.randomUUID()
    val nSucceeded = 1
    val nFailed = 2
    val nConnections = 5
    val actorSyncJobInfo = ActorSyncJobInfo(nSucceeded, nFailed, nConnections)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { rolloutActorFinder.getSyncInfoForPinnedActors(connectorRollout) } returns mapOf(actorId to actorSyncJobInfo)

    val result = connectorRolloutHandler.getActorSyncInfo(rolloutId)

    assertEquals(1, result.size)
    assertEquals(actorId, result.values.first().actorId)
    assertEquals(nConnections, result.values.first().getNumConnections())
    assertEquals(nSucceeded, result.values.first().getNumSucceeded())
    assertEquals(nFailed, result.values.first().getNumFailed())
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

  private fun createMockConnectorRolloutRequestBody(
    rolloutId: UUID,
    rolloutStrategy: ConnectorRolloutStrategy,
    actorIds: List<UUID>?,
    targetPercentage: Int?,
  ): ConnectorRolloutRequestBody =
    ConnectorRolloutRequestBody()
      .id(rolloutId)
      .rolloutStrategy(rolloutStrategy)
      .actorIds(actorIds)
      .targetPercentage(targetPercentage)

  private fun createMockConnectorRolloutFinalizeRequestBody(
    rolloutId: UUID,
    state: ConnectorRolloutStateTerminal,
    rolloutStrategy: ConnectorRolloutStrategy,
    errorMsg: String,
    failedReason: String,
    retainPinsOnCancellation: Boolean,
  ): ConnectorRolloutFinalizeRequestBody =
    ConnectorRolloutFinalizeRequestBody()
      .id(rolloutId)
      .state(state)
      .rolloutStrategy(rolloutStrategy)
      .errorMsg(errorMsg)
      .failedReason(failedReason)
      .retainPinsOnCancellation(retainPinsOnCancellation)

  private fun createMockActorDefinitionVersion(): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withVersionId(RELEASE_CANDIDATE_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
}
