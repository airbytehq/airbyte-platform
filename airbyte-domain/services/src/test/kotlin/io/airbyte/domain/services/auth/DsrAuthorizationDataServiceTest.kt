/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class DsrAuthorizationDataServiceTest {
  @Test
  fun `request presence never loads the sensitive deletion manifest and preserves storage failure`() {
    val requests = mockk<DataSubjectDeletionRequestRepository>()
    val service = DsrAuthorizationDataService(requests)
    val id = UUID(0, 1)
    every { requests.existsById(id) } returns true
    assertTrue(service.exists(id))
    every { requests.existsById(id) } returns false
    assertFalse(service.exists(id))
    val failure = IOException("deletion request storage unavailable")
    every { requests.existsById(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.exists(id) })
    verify(exactly = 0) { requests.findById(any()) }
  }
}
