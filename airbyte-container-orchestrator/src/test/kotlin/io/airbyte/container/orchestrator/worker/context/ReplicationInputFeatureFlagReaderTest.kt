/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.context

import io.airbyte.featureflag.DestinationTimeoutEnabled
import io.airbyte.persistence.job.models.ReplicationInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class ReplicationInputFeatureFlagReaderTest {
  @Test
  fun testReadFromReplicationInput() {
    val featureFlagMap = mapOf(DestinationTimeoutEnabled.key to true)
    val replicationInput =
      mockk<ReplicationInput> {
        every { featureFlags } returns featureFlagMap
      }
    val reader = ReplicationInputFeatureFlagReader(replicationInput)
    Assertions.assertEquals(true, reader.read(DestinationTimeoutEnabled))
  }

  @Test
  fun testReadFromReplicationInputMissingFlagReturnsDefault() {
    val featureFlagMap = emptyMap<String, Any>()
    val replicationInput =
      mockk<ReplicationInput> {
        every { featureFlags } returns featureFlagMap
      }
    val reader = ReplicationInputFeatureFlagReader(replicationInput)
    Assertions.assertEquals(DestinationTimeoutEnabled.default, reader.read(DestinationTimeoutEnabled))
  }
}
