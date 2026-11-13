/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class DirectCheckAllowlistTest {
  private val dataplaneGroupId = UUID.randomUUID()
  private val dispatchedFrom = LauncherDataplaneGroup.Known(dataplaneGroupId)

  private val featureFlagClient =
    mockk<FeatureFlagClient> {
      every { stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns " source-postgres , source-mysql "
    }
  private val allowlist = DirectCheckAllowlist(featureFlagClient)

  @Test
  fun `the connectors are the flag's value for the launcher's data plane group`() {
    assertEquals(setOf("source-postgres", "source-mysql"), allowlist.connectors(dispatchedFrom))
  }

  @Test
  fun `any version of Airbyte's image of a listed connector is allowed, and nothing else`() {
    assertTrue(allowlist.allows("airbyte/source-postgres:3.8.10", dispatchedFrom))
    assertTrue(allowlist.allows("airbyte/source-postgres:3.8.9", dispatchedFrom))
    assertFalse(allowlist.allows("airbyte/source-mssql:4.1.0", dispatchedFrom))
    assertFalse(allowlist.allows("acme/source-postgres:3.8.10", dispatchedFrom))
  }

  @Test
  fun `the flag is read on every call, so a change applies without a restart`() {
    assertTrue(allowlist.allows("airbyte/source-mysql:3.50.0", dispatchedFrom))

    every { featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns "source-postgres"

    assertFalse(allowlist.allows("airbyte/source-mysql:3.50.0", dispatchedFrom))
  }

  @Test
  fun `a launcher that sent no data plane group gets nothing, and the flag isn't read`() {
    assertEquals(emptySet<String>(), allowlist.connectors(LauncherDataplaneGroup.Unknown))
    assertFalse(allowlist.allows("airbyte/source-postgres:3.8.10", LauncherDataplaneGroup.Unknown))
    verify(exactly = 0) { featureFlagClient.stringVariation(any(), any()) }
  }

  @Test
  fun `an empty flag allows nothing`() {
    every { featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns ""

    assertFalse(allowlist.allows("airbyte/source-postgres:3.8.10", dispatchedFrom))
  }
}
