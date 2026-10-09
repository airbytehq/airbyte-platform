/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class OrganizationSemanticSearchServiceTest {
  private val repository = mockk<OrganizationRepository>()
  private val service = OrganizationSemanticSearchService(repository)
  private val organizationId = OrganizationId(UUID.randomUUID())

  @Test
  fun `reads return the stored value or empty`() {
    for (stored in listOf(Optional.of(true), Optional.of(false), Optional.empty())) {
      every { repository.findSemanticSearchEnabledById(organizationId.value) } returns stored
      assertEquals(stored, service.getEnablement(organizationId))
    }
  }

  @Test
  fun `updates write the requested boolean`() {
    for (enabled in listOf(true, false)) {
      every { repository.updateSemanticSearchEnabledById(organizationId.value, enabled) } returns 1L
      assertTrue(service.updateEnablement(organizationId, enabled))
      verify { repository.updateSemanticSearchEnabledById(organizationId.value, enabled) }
    }
  }

  @Test
  fun `missing or deleted update target reports not found`() {
    every { repository.updateSemanticSearchEnabledById(organizationId.value, false) } returns 0L
    assertFalse(service.updateEnablement(organizationId, false))
  }
}
