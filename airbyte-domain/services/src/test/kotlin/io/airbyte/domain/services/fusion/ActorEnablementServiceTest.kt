/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.data.services.impls.jooq.ActorEnablementPersistence
import io.airbyte.domain.models.fusion.ActorEnablementFlags
import io.airbyte.domain.models.fusion.ActorEnablementState
import io.airbyte.domain.models.fusion.EnablementActorType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ActorEnablementServiceTest {
  private val persistence = mockk<ActorEnablementPersistence>()
  private val service = ActorEnablementService(persistence, mockk(), mockk(), mockk())
  private val state =
    ActorEnablementState(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), EnablementActorType.SOURCE, ActorEnablementFlags())

  @Test
  fun `PUT compares expected state and preserves independent values`() {
    val current = state.copy(flags = ActorEnablementFlags(true, true))
    every {
      persistence.compareAndSet(state.organizationId, state.workspaceId, state.actorId, "source", current.flags, ActorEnablementFlags(true))
    } returns true
    assertEquals(ActorEnablementFlags(true), service.update(current, current.flags, ActorEnablementFlags(true)).flags)
    every { persistence.compareAndSet(any(), any(), any(), any(), ActorEnablementFlags(), any()) } returns false
    assertThrows<StateConflictProblem> { service.update(current, ActorEnablementFlags(), ActorEnablementFlags()) }
  }

  @Test
  fun `read cannot cross tenant boundary and database failures propagate`() {
    every { persistence.read(state.organizationId, state.actorId, "source") } returns null
    assertThrows<ResourceNotFoundProblem> { service.read(state.organizationId, state.actorId, EnablementActorType.SOURCE) }
    every { persistence.read(state.organizationId, state.actorId, "source") } throws IllegalStateException("database unavailable")
    assertThrows<IllegalStateException> { service.read(state.organizationId, state.actorId, EnablementActorType.SOURCE) }
  }
}
