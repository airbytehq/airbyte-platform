package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.commons.server.validation.InvalidRequest
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.MethodSource
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

class ActiveStatesProvider : ArgumentsProvider {
  override fun provideArguments(context: org.junit.jupiter.api.extension.ExtensionContext): Stream<out Arguments> {
    val terminalStateLiterals =
      ConnectorRolloutFinalState.entries
        .map { it.value() }
        .toSet()

    return ConnectorRolloutStateType.entries
      .filterNot {
        it.literal in terminalStateLiterals
      }
      .map { Arguments.of(it) }
      .stream()
  }
}

internal class ConnectorRolloutHandlerTest {
  private val connectorRolloutService = mockk<ConnectorRolloutService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val connectorRolloutHandler =
    ConnectorRolloutHandler(
      connectorRolloutService,
      actorDefinitionService,
      actorDefinitionVersionUpdater,
    )

  companion object {
    val DOCKER_REPOSITORY = "airbyte/source-faker"
    val DOCKER_IMAGE_TAG = "0.1"
    val ACTOR_DEFINITION_ID = UUID.randomUUID()
    val RELEASE_CANDIDATE_VERSION_ID = UUID.randomUUID()

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

    val rolloutRead = connectorRolloutHandler.getConnectorRollout(rolloutId)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), rolloutRead)

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test insertConnectorRollout`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { actorDefinitionService.getActorDefinitionVersion(any(), any()) } returns Optional.of(createMockActorDefinitionVersion())
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.of(createMockActorDefinitionVersion())
    every { connectorRolloutService.writeConnectorRollout(any()) } returns expectedRollout
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns emptyList()

    val insertedRolloutRead = connectorRolloutHandler.insertConnectorRollout(connectorRolloutCreate)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), insertedRolloutRead)

    verifyAll {
      connectorRolloutService.writeConnectorRollout(any())
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
      actorDefinitionService.getActorDefinitionVersion(any(), any())
      connectorRolloutService.listConnectorRollouts(any(), any())
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

    ConnectorRollout::class.memberProperties
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

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)

    assertEquals(expectedRollouts.map { connectorRolloutHandler.buildConnectorRolloutRead(it) }, rolloutReads)

    verify { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) }
    verify { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID) }
  }

  @Test
  fun `test listConnectorRolloutsNoActorDefinitionVersion`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) } returns Optional.empty()

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(actorDefinitionId, DOCKER_IMAGE_TAG)

    assertEquals(emptyList<ConnectorRolloutRead>(), rolloutReads)

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) }
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

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) }
  }

  @Test
  fun `test validateRolloutActorDefinitionId with mismatched repository throws exception`() {
    val mockActorDefinitionVersion = createMockActorDefinitionVersion()
    val dockerRepository = "wrong/repo"
    val actorDefinitionId = createMockActorDefinitionVersion().actorDefinitionId

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) } returns Optional.of(mockActorDefinitionVersion)

    assertThrows<InvalidRequest> {
      connectorRolloutHandler.validateRolloutActorDefinitionId(dockerRepository, DOCKER_IMAGE_TAG, actorDefinitionId)
    }

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG) }
  }

  @ParameterizedTest
  @MethodSource("validInsertStates")
  fun `test insertConnectorRolloutExistingCanceledRolloutSucceeds`(state: ConnectorEnumRolloutState) {
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID()).apply { this.state = state }

    val connectorRolloutHandlerSpy = spyk<ConnectorRolloutHandler>(connectorRolloutHandler)
    every { connectorRolloutHandlerSpy.validateRolloutActorDefinitionId(any(), any(), any()) } just Runs
    every { actorDefinitionService.getActorDefinitionVersion(any(), any()) } returns Optional.of(createMockActorDefinitionVersion())
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()

    val result = connectorRolloutHandlerSpy.insertConnectorRollout(connectorRolloutCreate)
    assertEquals(connectorRollout.state.toString(), result.state.toString())
    assertEquals(connectorRollout.actorDefinitionId, result.actorDefinitionId)
    assertEquals(connectorRollout.releaseCandidateVersionId, result.releaseCandidateVersionId)
    assertEquals(connectorRollout.initialVersionId, result.initialVersionId)
    assertEquals(connectorRollout.rolloutStrategy.toString(), result.rolloutStrategy.toString())

    verify {
      connectorRolloutHandlerSpy.validateRolloutActorDefinitionId(any(), any(), any())
      connectorRolloutService.listConnectorRollouts(any(), any())
      connectorRolloutHandlerSpy.getAndValidateInsertRequest(any())
      actorDefinitionService.getActorDefinitionVersion(any(), any())
    }
  }

  @ParameterizedTest
  @MethodSource("invalidInsertStates")
  fun `test insertConnectorRolloutExistingNonCanceledRolloutThrows`(state: ConnectorEnumRolloutState) {
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID()).apply { this.state = state }

    val connectorRolloutHandlerSpy = spyk<ConnectorRolloutHandler>(connectorRolloutHandler)
    every { connectorRolloutHandlerSpy.validateRolloutActorDefinitionId(any(), any(), any()) } just Runs
    every { actorDefinitionService.getActorDefinitionVersion(any(), any()) } returns Optional.of(createMockActorDefinitionVersion())
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns connectorRollout
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()

    assertThrows<InvalidRequest> { connectorRolloutHandlerSpy.insertConnectorRollout(connectorRolloutCreate) }

    verify {
      connectorRolloutHandlerSpy.validateRolloutActorDefinitionId(any(), any(), any())
      connectorRolloutService.listConnectorRollouts(any(), any())
      connectorRolloutHandlerSpy.getAndValidateInsertRequest(any())
      actorDefinitionService.getActorDefinitionVersion(any(), any())
    }
  }

  @ParameterizedTest
  @MethodSource("validStartStates")
  fun `test getAndValidateStartRequest with initialized state`(state: ConnectorEnumRolloutState) {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId).apply { this.state = state }

    every { actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG) } returns
      Optional.of(
        createMockActorDefinitionVersion(),
      )
    every { connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID) } returns listOf(connectorRollout)

    val result =
      connectorRolloutHandler.getAndValidateStartRequest(
        ConnectorRolloutStartRequestBody()
          .actorDefinitionId(ACTOR_DEFINITION_ID)
          .dockerRepository(DOCKER_REPOSITORY)
          .dockerImageTag(DOCKER_IMAGE_TAG)
          .rolloutStrategy(ConnectorRolloutStrategy.MANUAL),
      )

    assertEquals(connectorRollout, result)

    verify {
      actorDefinitionService.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
      connectorRolloutService.listConnectorRollouts(ACTOR_DEFINITION_ID, RELEASE_CANDIDATE_VERSION_ID)
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

    every { actorDefinitionService.getActorDefinitionVersion(any(), any()) } returns Optional.of(createMockActorDefinitionVersion())
    every { connectorRolloutService.listConnectorRollouts(any(), any()) } returns listOf(connectorRollout)

    assertThrows<InvalidRequest> {
      connectorRolloutHandler.getAndValidateStartRequest(createMockConnectorRolloutStartRequestBody())
    }

    verify {
      actorDefinitionService.getActorDefinitionVersion(any(), any())
      connectorRolloutService.listConnectorRollouts(any(), any())
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

    assertThrows<InvalidRequest> {
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

    assertThrows<InvalidRequest> {
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

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = ACTOR_DEFINITION_ID,
    releaseCandidateVersionId: UUID = RELEASE_CANDIDATE_VERSION_ID,
  ): ConnectorRollout {
    return ConnectorRollout().apply {
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
  }

  private fun createMockConnectorRolloutCreateRequestBody(): ConnectorRolloutCreateRequestBody {
    return ConnectorRolloutCreateRequestBody()
      .dockerRepository(DOCKER_REPOSITORY)
      .dockerImageTag(DOCKER_IMAGE_TAG)
      .actorDefinitionId(ACTOR_DEFINITION_ID)
      .initialRolloutPct(10)
      .finalTargetRolloutPct(100)
      .hasBreakingChanges(false)
      .maxStepWaitTimeMins(60)
      .expiresAt(OffsetDateTime.now().plusDays(1))
  }

  private fun createMockConnectorRolloutStartRequestBody(): ConnectorRolloutStartRequestBody {
    return ConnectorRolloutStartRequestBody()
      .dockerRepository(DOCKER_REPOSITORY)
      .dockerImageTag(DOCKER_IMAGE_TAG)
      .actorDefinitionId(ACTOR_DEFINITION_ID)
      .workflowRunId(UUID.randomUUID().toString())
      .rolloutStrategy(ConnectorRolloutStrategy.MANUAL)
  }

  private fun createMockConnectorRolloutRequestBody(
    rolloutId: UUID,
    rolloutStrategy: ConnectorRolloutStrategy,
  ): ConnectorRolloutRequestBody {
    return ConnectorRolloutRequestBody()
      .id(rolloutId)
      .rolloutStrategy(rolloutStrategy)
      .actorIds(listOf(UUID.randomUUID()))
  }

  private fun createMockConnectorRolloutFinalizeRequestBody(
    rolloutId: UUID,
    state: ConnectorRolloutStateTerminal,
    rolloutStrategy: ConnectorRolloutStrategy,
    errorMsg: String,
    failedReason: String,
  ): ConnectorRolloutFinalizeRequestBody {
    return ConnectorRolloutFinalizeRequestBody()
      .id(rolloutId)
      .state(state)
      .rolloutStrategy(rolloutStrategy)
      .errorMsg(errorMsg)
      .failedReason(failedReason)
  }

  private fun createMockActorDefinitionVersion(): ActorDefinitionVersion {
    return ActorDefinitionVersion()
      .withVersionId(RELEASE_CANDIDATE_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
  }
}
