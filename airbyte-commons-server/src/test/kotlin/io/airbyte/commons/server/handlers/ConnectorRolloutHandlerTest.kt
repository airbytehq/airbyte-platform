package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.client.ConnectorRolloutClient
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.ScopedConfigurationService
import io.mockk.clearAllMocks
import io.mockk.every
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
  private val connectorRolloutClient = mockk<ConnectorRolloutClient>()
  private val connectorRolloutHandler =
    ConnectorRolloutHandler(
      connectorRolloutService,
      actorDefinitionService,
      actorDefinitionVersionUpdater,
      connectorRolloutClient,
      userPersistence,
    )

  companion object {
    const val DOCKER_REPOSITORY = "airbyte/source-faker"
    const val DOCKER_IMAGE_TAG = "0.1"
    val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    val RELEASE_CANDIDATE_VERSION_ID: UUID = UUID.randomUUID()
    val UPDATED_BY = UUID.randomUUID()

    @JvmStatic
    fun validInsertStates() = listOf(ConnectorEnumRolloutState.CANCELED_ROLLED_BACK)

    @JvmStatic
    fun invalidInsertStates() = ConnectorEnumRolloutState.entries.filter { it != ConnectorEnumRolloutState.CANCELED_ROLLED_BACK }

    @JvmStatic
    fun validStartStates() = listOf(ConnectorEnumRolloutState.INITIALIZED)

    @JvmStatic
    fun invalidStartStates() = ConnectorEnumRolloutState.entries.filter { it != ConnectorEnumRolloutState.INITIALIZED }

    @JvmStatic
    fun validUpdateStates() =
      setOf(
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
    fun workflowStartedInProgress() =
      listOf(ConnectorEnumRolloutState.WORKFLOW_STARTED, ConnectorEnumRolloutState.IN_PROGRESS, ConnectorEnumRolloutState.PAUSED)
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

    val rolloutRead = connectorRolloutHandler.getConnectorRollout(rolloutId)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), rolloutRead)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test doConnectorRollout with scoped configurations insertion`() {
    val rolloutId = UUID.randomUUID()
    val rolloutStrategy = ConnectorRolloutStrategy.MANUAL
    val actorDefinitionId = UUID.randomUUID()
    val initialVersionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val actorIds = listOf(UUID.randomUUID(), UUID.randomUUID())

    val initialRollout =
      createMockConnectorRollout(
        id = rolloutId,
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
      ).apply {
        this.initialVersionId = initialVersionId
        this.state = ConnectorEnumRolloutState.WORKFLOW_STARTED
      }

    val connectorRolloutRequestBody =
      createMockConnectorRolloutRequestBody(rolloutId, rolloutStrategy).apply {
        this.actorIds = actorIds
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns initialRollout
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()

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

    assertEquals(expectedRollouts.map { connectorRolloutHandler.buildConnectorRolloutRead(it) }, rolloutReads)

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

    assertEquals(expectedRollouts.map { connectorRolloutHandler.buildConnectorRolloutRead(it) }, rolloutReads)

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

    assertEquals(expectedRollouts.map { connectorRolloutHandler.buildConnectorRolloutRead(it) }, rolloutReads)

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
  fun `test getAndValidateStartRequest with invalid state throws exception`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = state
      }

    every { connectorRolloutService.getConnectorRollout(any()) } returns connectorRollout

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getAndValidateStartRequest(createMockConnectorRolloutStartRequestBody())
    }

    verify {
      connectorRolloutService.getConnectorRollout(any())
    }
  }

  @ParameterizedTest
  @MethodSource("validUpdateStates")
  fun `test getAndRollOutConnectorRollout with valid state`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout =
      createMockConnectorRollout(rolloutId).apply {
        this.state = ConnectorEnumRolloutState.IN_PROGRESS
      }

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { scopedConfigurationService.listScopedConfigurationsWithScopes(any(), any(), any(), any(), any()) } returns emptyList()
    every { actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any()) } returns Unit

    val result =
      connectorRolloutHandler.getAndRollOutConnectorRollout(
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL),
      )

    assertEquals(connectorRollout, result)

    verify {
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(any(), any(), any(), any())
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
        createMockConnectorRolloutRequestBody(rolloutId, ConnectorRolloutStrategy.MANUAL),
      )
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
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
      connectorRolloutHandler.getAndValidateUpdateStateRequest(rolloutId, ConnectorEnumRolloutState.FINALIZING, null, null)

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
      connectorRolloutHandler.getAndValidateUpdateStateRequest(rolloutId, ConnectorEnumRolloutState.SUCCEEDED, null, null)
    }

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test manualStartConnectorRollout`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutWorkflowStart =
      ConnectorRolloutManualStartRequestBody().apply {
        dockerRepository = DOCKER_REPOSITORY
        dockerImageTag = DOCKER_IMAGE_TAG
        actorDefinitionId = ACTOR_DEFINITION_ID
        updatedBy = UPDATED_BY
      }
    val connectorRollout = createMockConnectorRollout(rolloutId)

    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutClient.startRollout(any()) } returns ConnectorRolloutOutput(state = ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()
    every {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    } returns Optional.of(createMockActorDefinitionVersion())
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns Optional.of(createMockActorDefinitionVersion())

    val result = connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutWorkflowStart)

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      connectorRolloutClient.startRollout(any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      connectorRolloutService.listConnectorRollouts(any(), any())
      actorDefinitionService.getActorDefinitionVersion(any())
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    }
  }

  @ParameterizedTest
  @MethodSource("workflowStartedInProgress")
  fun `test manualDoConnectorRolloutWorkflowUpdate`(state: ConnectorEnumRolloutState) {
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

    every { connectorRolloutClient.doRollout(any()) } returns ConnectorRolloutOutput(state = ConnectorEnumRolloutState.IN_PROGRESS)
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    val result = connectorRolloutHandler.manualDoConnectorRolloutUpdate(connectorRolloutWorkflowUpdate)

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      connectorRolloutClient.doRollout(any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionService.getActorDefinitionVersion(any())
    }

    // Verify that startWorkflow() was not called because the rollout is already in progress
    verify(exactly = 0) { connectorRolloutClient.startRollout(any()) }
  }

  @Test
  fun `test manualDoConnectorRolloutWorkflowUpdate workflow not started`() {
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
    // Rollout has been initialized, but workflow hasn't been started
    connectorRollout.apply { this.state = ConnectorEnumRolloutState.INITIALIZED }

    every { connectorRolloutClient.startRollout(any()) } returns ConnectorRolloutOutput(state = ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { connectorRolloutClient.doRollout(any()) } returns ConnectorRolloutOutput(state = ConnectorEnumRolloutState.IN_PROGRESS)
    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { actorDefinitionService.getActorDefinitionVersion(any()) } returns createMockActorDefinitionVersion()

    val result = connectorRolloutHandler.manualDoConnectorRolloutUpdate(connectorRolloutWorkflowUpdate)

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      connectorRolloutClient.startRollout(any())
      connectorRolloutClient.doRollout(any())
      connectorRolloutService.getConnectorRollout(rolloutId)
      actorDefinitionService.getActorDefinitionVersion(any())
    }
  }

  @Test
  fun `test manualFinalizeConnectorRolloutWorkflowUpdate`() {
    val rolloutId = UUID.randomUUID()
    val state = ConnectorRolloutStateTerminal.SUCCEEDED
    val connectorRolloutFinalizeWorkflowUpdate =
      ConnectorRolloutManualFinalizeRequestBody().apply {
        dockerRepository = "airbyte/source-faker"
        dockerImageTag = "0.1"
        actorDefinitionId = UUID.randomUUID()
        id = rolloutId
        this.state = state
      }

    every { connectorRolloutClient.finalizeRollout(any()) } returns Unit

    connectorRolloutHandler.manualFinalizeConnectorRolloutWorkflowUpdate(connectorRolloutFinalizeWorkflowUpdate)

    verifyAll {
      connectorRolloutClient.finalizeRollout(any())
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

    val result = connectorRolloutHandler.getOrCreateAndValidateManualStartInput(DOCKER_REPOSITORY, actorDefinitionId, dockerImageTag, UPDATED_BY)

    assertEquals(connectorRollout.id, result.id)
    verifyAll {
      actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
      connectorRolloutService.listConnectorRollouts(actorDefinitionId, actorDefinitionVersion.versionId)
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
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
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    } returns null

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(
        DOCKER_REPOSITORY,
        actorDefinitionId,
        dockerImageTag,
        UPDATED_BY,
      )
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

    connectorRolloutHandler.getOrCreateAndValidateManualStartInput(DOCKER_REPOSITORY, ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG, UPDATED_BY)

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
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(DOCKER_REPOSITORY, ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG, UPDATED_BY)
    }

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
  }

  @Test
  fun `test getOrCreateAndValidateManualStartInput throws ConnectorRolloutInvalidRequestProblem when docker repository doesn't match`() {
    val dockerRepository = "wrong-repo"
    val actorDefinitionVersion = createMockActorDefinitionVersion()

    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns Optional.of(actorDefinitionVersion)

    assertThrows<ConnectorRolloutInvalidRequestProblem> {
      connectorRolloutHandler.getOrCreateAndValidateManualStartInput(dockerRepository, ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG, UPDATED_BY)
    }

    verifyAll {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
    }
  }

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = ACTOR_DEFINITION_ID,
    releaseCandidateVersionId: UUID = RELEASE_CANDIDATE_VERSION_ID,
  ): ConnectorRollout =
    ConnectorRollout().apply {
      this.id = id
      this.actorDefinitionId = actorDefinitionId
      this.releaseCandidateVersionId = releaseCandidateVersionId
      this.initialVersionId = UUID.randomUUID()
      this.state = ConnectorEnumRolloutState.INITIALIZED
      this.initialRolloutPct = 10L
      this.finalTargetRolloutPct = 100L
      this.hasBreakingChanges = false
      this.rolloutStrategy = ConnectorEnumRolloutStrategy.MANUAL
      this.maxStepWaitTimeMins = 60L
      this.createdAt = OffsetDateTime.now().toEpochSecond()
      this.updatedAt = OffsetDateTime.now().toEpochSecond()
      this.expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond()
    }

  private fun createMockConnectorRolloutStartRequestBody(): ConnectorRolloutStartRequestBody =
    ConnectorRolloutStartRequestBody()
      .id(UUID.randomUUID())
      .workflowRunId(UUID.randomUUID().toString())
      .rolloutStrategy(ConnectorRolloutStrategy.MANUAL)

  private fun createMockConnectorRolloutRequestBody(
    rolloutId: UUID,
    rolloutStrategy: ConnectorRolloutStrategy,
  ): ConnectorRolloutRequestBody =
    ConnectorRolloutRequestBody()
      .id(rolloutId)
      .rolloutStrategy(rolloutStrategy)
      .actorIds(listOf(UUID.randomUUID()))

  private fun createMockConnectorRolloutFinalizeRequestBody(
    rolloutId: UUID,
    state: ConnectorRolloutStateTerminal,
    rolloutStrategy: ConnectorRolloutStrategy,
    errorMsg: String,
    failedReason: String,
  ): ConnectorRolloutFinalizeRequestBody =
    ConnectorRolloutFinalizeRequestBody()
      .id(rolloutId)
      .state(state)
      .rolloutStrategy(rolloutStrategy)
      .errorMsg(errorMsg)
      .failedReason(failedReason)

  private fun createMockActorDefinitionVersion(): ActorDefinitionVersion =
    ActorDefinitionVersion()
      .withVersionId(RELEASE_CANDIDATE_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
}
