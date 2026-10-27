/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRollout
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class RolloutAuthorizationDataServiceTest {
  private val data = mockk<AuthorizationDataService>()
  private val rollouts = mockk<ConnectorRolloutService>()
  private val definitions = mockk<ActorDefinitionService>()
  private val service = RolloutAuthorizationDataService(data, rollouts, definitions)
  private val id = UUID(0, 1)
  private val definition = UUID(0, 2)
  private val candidate = UUID(0, 3)
  private val initial = UUID(0, 4)
  private val actor = UUID(0, 5)
  private val workspace = UUID(0, 6)
  private val organization = UUID(0, 7)
  private val owner = WorkspaceAndOrganization(workspace, organization)

  private fun version(id: UUID) =
    ActorDefinitionVersion()
      .withVersionId(id)
      .withActorDefinitionId(definition)
      .withDockerRepository("fixture/repo")
      .withDockerImageTag("1.0")

  private fun rollout() =
    ConnectorRollout(
      id,
      actorDefinitionId = definition,
      releaseCandidateVersionId = candidate,
      initialVersionId = initial,
      state = ConnectorEnumRolloutState.IN_PROGRESS,
      hasBreakingChanges = false,
      createdAt = 0,
      updatedAt = 0,
      tag = null,
    )

  private fun metadata() = RolloutAuthorizationMetadata(id, definition, candidate, initial, "fixture/repo", "1.0")

  @Test
  fun `actor organization must agree with its saved workspace before definition pins`() {
    every { data.actorWorkspaceAndOrganization(actor) } returns owner
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns owner.copy(organizationId = UUID(0, 99))
    every { data.savedActorDefinitionId(actor, workspace) } returns definition
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireActors(metadata(), listOf(actor)) }
  }

  @Test
  fun `saved rollout requires exact ID and both version parents without absorbing dependency errors`() {
    val saved = rollout()
    every { rollouts.getConnectorRollout(id) } returns saved
    every { data.requireSavedActorDefinition(definition) } just Runs
    every { definitions.getActorDefinitionVersion(candidate) } returns version(candidate)
    every { definitions.getActorDefinitionVersion(initial) } returns version(initial)
    assertEquals(metadata(), service.saved(id))
    every { definitions.getActorDefinitionVersion(initial) } returns version(initial).withActorDefinitionId(UUID(0, 99))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.saved(id) }
    val failure = IOException("rollout unavailable")
    every { rollouts.getConnectorRollout(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.saved(id) })
    every { rollouts.getConnectorRollout(id) } throws ConfigNotFoundException("ConnectorRollout", id.toString())
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.saved(id) }
  }

  @Test
  fun `definition tag fallback requires exactly one nonterminal saved rollout and matching candidate`() {
    val saved = rollout()
    every { definitions.getActorDefinitionVersion(definition, "1.0") } returns Optional.of(version(candidate))
    every { rollouts.listConnectorRollouts(definition, candidate) } returns listOf(saved)
    every { rollouts.getConnectorRollout(id) } returns saved
    every { data.requireSavedActorDefinition(definition) } just Runs
    every { definitions.getActorDefinitionVersion(candidate) } returns version(candidate)
    every { definitions.getActorDefinitionVersion(initial) } returns version(initial)
    assertEquals(metadata(), service.byDefinitionAndTag(definition, "1.0"))
    every { rollouts.listConnectorRollouts(definition, candidate) } returns listOf(saved, saved.copy(id = UUID(0, 99)))
    assertThrows(RolloutReferenceMismatchException::class.java) { service.byDefinitionAndTag(definition, "1.0") }
    every { rollouts.listConnectorRollouts(definition, candidate) } returns listOf(saved.copy(state = ConnectorEnumRolloutState.SUCCEEDED))
    assertThrows(RolloutReferenceMismatchException::class.java) { service.byDefinitionAndTag(definition, "1.0") }
    every { definitions.getActorDefinitionVersion(definition, "1.0") } returns Optional.empty()
    assertThrows(RolloutReferenceMismatchException::class.java) { service.byDefinitionAndTag(definition, "1.0") }
  }
}
