/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.config.AttributeName
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.Operator
import io.airbyte.data.repositories.entities.ConnectorRollout
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

class ActiveStatesProvider : ArgumentsProvider {
  override fun provideArguments(context: org.junit.jupiter.api.extension.ExtensionContext): Stream<out Arguments> {
    val terminalStateLiterals =
      ConnectorRolloutFinalState.entries
        .map { it.value() }
        .toSet()

    return ConnectorRolloutStateType.entries
      .filter { stateType ->
        !terminalStateLiterals.contains(stateType.literal)
      }.map { Arguments.of(it) }
      .stream()
  }
}

class TerminalStatesProvider : ArgumentsProvider {
  override fun provideArguments(context: org.junit.jupiter.api.extension.ExtensionContext): Stream<out Arguments> {
    val terminalStateLiterals =
      ConnectorRolloutFinalState.entries
        .map { it.value() }
        .toSet()

    return ConnectorRolloutStateType.entries
      .filter { stateType ->
        terminalStateLiterals.contains(stateType.literal)
      }.map { Arguments.of(it) }
      .stream()
  }
}

@MicronautTest
internal class ConnectorRolloutRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      jooqDslContext
        .alterTable(Tables.CONNECTOR_ROLLOUT)
        .dropForeignKey(Keys.CONNECTOR_ROLLOUT__FK_ACTOR_DEFINITION_ID.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.CONNECTOR_ROLLOUT)
        .dropForeignKey(Keys.CONNECTOR_ROLLOUT__FK_RELEASE_CANDIDATE_VERSION_ID.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.CONNECTOR_ROLLOUT)
        .dropForeignKey(Keys.CONNECTOR_ROLLOUT__FK_INITIAL_VERSION_ID.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.CONNECTOR_ROLLOUT)
        .dropForeignKey(Keys.CONNECTOR_ROLLOUT__FK_UPDATED_BY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    connectorRolloutRepository.deleteAll()
  }

  @ParameterizedTest
  @ArgumentsSource(TerminalStatesProvider::class)
  fun `test successful db insertion for terminal states`(state: ConnectorRolloutStateType) {
    val rolloutId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val rollout = createConnectorRollout(rolloutId, actorDefinitionId, state)

    connectorRolloutRepository.save(rollout)
    assertEquals(1, connectorRolloutRepository.count())

    val persistedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertConnectorRolloutEquals(rollout, persistedRollout)
  }

  @ParameterizedTest
  @ArgumentsSource(ActiveStatesProvider::class)
  fun `test db insertion fails when actor has active rollouts with same tag`(state: ConnectorRolloutStateType) {
    val rolloutId1 = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val tag = "tag"
    val rollout1 = createConnectorRollout(rolloutId1, actorDefinitionId, state, tag)

    connectorRolloutRepository.save(rollout1)
    assertEquals(1, connectorRolloutRepository.count())

    // Attempt to insert another rollout with the same actorDefinitionId and an active state fails because only one active rollout is allowed per actor
    val rolloutId2 = UUID.randomUUID()
    val rollout2 = createConnectorRollout(rolloutId2, actorDefinitionId, state, tag)

    assertThrows<Exception> {
      connectorRolloutRepository.save(rollout2)
    }
  }

  @Test
  fun `test db insertion succeeds when actor has no active rollouts`() {
    val actorDefinitionId = UUID.randomUUID()
    val rolloutId1 = UUID.randomUUID()
    val rollout1 = createConnectorRollout(rolloutId1, actorDefinitionId, ConnectorRolloutStateType.succeeded)

    connectorRolloutRepository.save(rollout1)
    assertEquals(1, connectorRolloutRepository.count())

    // Inserting a new rollout with an active state is allowed
    val rolloutId2 = UUID.randomUUID()
    val rollout2 = createConnectorRollout(rolloutId2, actorDefinitionId, ConnectorRolloutStateType.in_progress)

    connectorRolloutRepository.save(rollout2)
    assertEquals(2, connectorRolloutRepository.count())
  }

  private fun createConnectorRollout(
    rolloutId: UUID,
    actorDefinitionId: UUID,
    state: ConnectorRolloutStateType,
    tag: String? = null,
  ): ConnectorRollout =
    ConnectorRollout(
      id = rolloutId,
      workflowRunId = UUID.randomUUID().toString(),
      actorDefinitionId = actorDefinitionId,
      releaseCandidateVersionId = UUID.randomUUID(),
      initialVersionId = UUID.randomUUID(),
      state = state,
      initialRolloutPct = 10,
      finalTargetRolloutPct = 100,
      hasBreakingChanges = false,
      rolloutStrategy = ConnectorRolloutStrategyType.manual,
      maxStepWaitTimeMins = 60,
      expiresAt = OffsetDateTime.now().plusDays(1),
      filters =
        io.airbyte.config
          .ConnectorRolloutFilters(
            customerTierFilters =
              listOf(
                CustomerTierFilter(
                  name = AttributeName.TIER,
                  operator = Operator.IN,
                  value = listOf(CustomerTier.TIER_1),
                ),
              ),
          ).toEntity(),
      tag = tag,
    )

  private fun assertConnectorRolloutEquals(
    expected: ConnectorRollout,
    actual: ConnectorRollout,
  ) {
    assertEquals(expected.id, actual.id)
    assertEquals(expected.workflowRunId, actual.workflowRunId)
    assertEquals(expected.actorDefinitionId, actual.actorDefinitionId)
    assertEquals(expected.releaseCandidateVersionId, actual.releaseCandidateVersionId)
    assertEquals(expected.initialVersionId, actual.initialVersionId)
    assertEquals(expected.state, actual.state)
    assertEquals(expected.initialRolloutPct, actual.initialRolloutPct)
    assertEquals(expected.finalTargetRolloutPct, actual.finalTargetRolloutPct)
    assertFalse(actual.hasBreakingChanges)
    assertEquals(expected.rolloutStrategy, actual.rolloutStrategy)
    assertEquals(expected.maxStepWaitTimeMins, actual.maxStepWaitTimeMins)
    assertEquals(0, actual.currentTargetRolloutPct)
    assertNull(actual.updatedBy)
    assertNull(actual.completedAt)
    assertNull(actual.errorMsg)
    assertNull(actual.failedReason)
  }

  @Test
  fun `test db update`() {
    val rolloutId = UUID.randomUUID()
    val initialState = ConnectorRolloutStateType.initialized
    val rollout =
      ConnectorRollout(
        id = rolloutId,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        state = initialState,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      )
    connectorRolloutRepository.save(rollout)
    val persistedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(initialState, persistedRollout.state)

    val newState = ConnectorRolloutStateType.in_progress
    rollout.state = newState
    connectorRolloutRepository.update(rollout)

    val updatedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(newState, updatedRollout.state)
  }

  @ParameterizedTest
  @ArgumentsSource(ActiveStatesProvider::class)
  fun `test db update from active state to active states`(initialState: ConnectorRolloutStateType) {
    val rolloutId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val rollout = createConnectorRollout(rolloutId, actorDefinitionId, initialState)

    // Save the initial rollout with an active state
    connectorRolloutRepository.save(rollout)
    val persistedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(initialState, persistedRollout.state)

    // Update to another active state
    val newState =
      if (initialState == ConnectorRolloutStateType.initialized) {
        ConnectorRolloutStateType.in_progress
      } else {
        ConnectorRolloutStateType.paused
      }
    rollout.state = newState
    connectorRolloutRepository.update(rollout)

    val updatedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(newState, updatedRollout.state)
  }

  @ParameterizedTest
  @ArgumentsSource(TerminalStatesProvider::class)
  fun `test db update from terminal state to terminal state`(initialState: ConnectorRolloutStateType) {
    val rolloutId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val rollout = createConnectorRollout(rolloutId, actorDefinitionId, initialState)

    // Save the initial rollout with a terminal state
    connectorRolloutRepository.save(rollout)
    val persistedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(initialState, persistedRollout.state)

    // Attempt to update to another terminal state
    val newState =
      if (initialState == ConnectorRolloutStateType.succeeded) {
        ConnectorRolloutStateType.errored
      } else {
        ConnectorRolloutStateType.failed_rolled_back
      }
    rollout.state = newState
    connectorRolloutRepository.update(rollout)

    val updatedRollout = connectorRolloutRepository.findById(rolloutId).get()
    assertEquals(newState, updatedRollout.state)
  }

  @Test
  fun `test prevent update from terminal to active state when active rollout exists for actor`() {
    val rolloutId1 = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val tag = "tag"
    val rollout1 = createConnectorRollout(rolloutId1, actorDefinitionId, ConnectorRolloutStateType.succeeded, tag)
    // Save the first rollout with a terminal state
    connectorRolloutRepository.save(rollout1)

    // Save a new rollout with an active state
    val rolloutId2 = UUID.randomUUID()
    val rollout2 = createConnectorRollout(rolloutId2, actorDefinitionId, ConnectorRolloutStateType.initialized, tag)
    connectorRolloutRepository.save(rollout2)

    // Attempt to update the first rollout to an active state fails because only one active rollout is allowed per actor
    rollout1.state = ConnectorRolloutStateType.in_progress
    assertThrows<Exception> {
      connectorRolloutRepository.update(rollout1)
    }
  }

  @Test
  fun `test db delete`() {
    val rolloutId = UUID.randomUUID()
    val rollout =
      ConnectorRollout(
        id = rolloutId,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      )

    connectorRolloutRepository.save(rollout)
    assertEquals(1, connectorRolloutRepository.count())

    connectorRolloutRepository.deleteById(rolloutId)
    assertEquals(0, connectorRolloutRepository.count())
  }

  @Test
  fun `test db insert same id throws`() {
    val rolloutId = UUID.randomUUID()
    val rollout1 =
      ConnectorRollout(
        id = rolloutId,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      )

    connectorRolloutRepository.save(rollout1)
    assertEquals(1, connectorRolloutRepository.count())

    val rollout2 =
      ConnectorRollout(
        id = rolloutId,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 20,
        finalTargetRolloutPct = 80,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      )

    assertThrows<DataAccessException> { connectorRolloutRepository.save(rollout2) }
  }

  @Test
  fun `test db list all`() {
    val actorDefinitionId1 = UUID.randomUUID()
    val actorDefinitionId2 = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val rolloutId1 = UUID.randomUUID()
    val rolloutId2 = UUID.randomUUID()

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId1,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId1,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    var persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId1,
        releaseCandidateVersionId,
      )

    assertEquals(1, persistedRollout.size)
    assertEquals(rolloutId1, persistedRollout[0].id)

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId2,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId2,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.succeeded,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    persistedRollout = connectorRolloutRepository.findAllOrderByUpdatedAtDesc()

    assertEquals(2, persistedRollout.size)
    assertEquals(rolloutId2, persistedRollout[0].id) // Newest by updatedAt
    assertEquals(rolloutId1, persistedRollout[1].id)
  }

  @Test
  fun `test db list by actor_definition_id`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId1 = UUID.randomUUID()
    val releaseCandidateVersionId2 = UUID.randomUUID()
    val rolloutId1 = UUID.randomUUID()
    val rolloutId2 = UUID.randomUUID()

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId1,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId1,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    var persistedRollout = connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(actorDefinitionId)

    assertEquals(1, persistedRollout.size)
    assertEquals(rolloutId1, persistedRollout[0].id)

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId2,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId2,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.succeeded,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    persistedRollout = connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(actorDefinitionId)

    assertEquals(2, persistedRollout.size)
    assertEquals(rolloutId2, persistedRollout[0].id) // Newest by updatedAt
    assertEquals(rolloutId1, persistedRollout[1].id)
  }

  @Test
  fun `test db get by actor_definition_id and release_candidate_version_id`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val rolloutId1 = UUID.randomUUID()
    val rolloutId2 = UUID.randomUUID()
    val rolloutId3 = UUID.randomUUID()
    val rolloutId4 = UUID.randomUUID()

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId1,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    var persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )

    assertEquals(1, persistedRollout.size)
    assertEquals(rolloutId1, persistedRollout[0].id)

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId2,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.succeeded,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )

    assertEquals(2, persistedRollout.size)
    assertEquals(rolloutId2, persistedRollout[0].id) // Newest by updatedAt
    assertEquals(rolloutId1, persistedRollout[1].id)

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId3,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    assertEquals(3, connectorRolloutRepository.count())

    persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )
    assertEquals(2, persistedRollout.size) // Only 2 rollouts with the same actor_definition_id and release_candidate_version_id
    assertEquals(rolloutId2, persistedRollout[0].id) // Newest by updatedAt
    assertEquals(rolloutId1, persistedRollout[1].id)

    connectorRolloutRepository.update(
      ConnectorRollout(
        id = rolloutId1,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.succeeded,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    assertEquals(3, connectorRolloutRepository.count())

    persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )
    assertEquals(2, persistedRollout.size) // Only 2 rollouts with the same actor_definition_id and release_candidate_version_id
    assertEquals(rolloutId1, persistedRollout[0].id) // Newest by updatedAt
    assertEquals(rolloutId2, persistedRollout[1].id)

    connectorRolloutRepository.save(
      ConnectorRollout(
        id = rolloutId4,
        workflowRunId = UUID.randomUUID().toString(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = releaseCandidateVersionId,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorRolloutStateType.initialized,
        initialRolloutPct = 10,
        finalTargetRolloutPct = 100,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorRolloutStrategyType.manual,
        maxStepWaitTimeMins = 60,
        expiresAt = OffsetDateTime.now().plusDays(1),
      ),
    )

    assertEquals(4, connectorRolloutRepository.count())

    persistedRollout =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )

    assertEquals(3, persistedRollout.size)
    assertEquals(rolloutId4, persistedRollout[0].id)
    assertEquals(rolloutId1, persistedRollout[1].id)
    assertEquals(rolloutId2, persistedRollout[2].id)
  }

  @Test
  fun `test db get non-existent rollout by actor_definition_id and release_candidate_version_id returns empty list`() {
    val persistedRollouts =
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
    assertTrue(persistedRollouts.isEmpty())
  }
}
