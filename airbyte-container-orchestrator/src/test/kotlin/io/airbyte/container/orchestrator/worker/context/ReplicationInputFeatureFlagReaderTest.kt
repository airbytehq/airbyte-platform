/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.context

import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.persistence.job.models.ReplicationInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class ReplicationInputFeatureFlagReaderTest {
  @Test
  fun testReadFromReplicationInput() {
    val featureFlagMap = mapOf(ShouldFailSyncOnDestinationTimeout.key to true)
    val replicationInput =
      mockk<ReplicationInput> {
        every { featureFlags } returns featureFlagMap
      }
    val reader = ReplicationInputFeatureFlagReader(replicationInput)
    Assertions.assertEquals(true, reader.read(ShouldFailSyncOnDestinationTimeout))
  }

  @Test
  fun testReadFromReplicationInputMissingFlagReturnsDefault() {
    val featureFlagMap = emptyMap<String, Any>()
    val replicationInput =
      mockk<ReplicationInput> {
        every { featureFlags } returns featureFlagMap
      }
    val reader = ReplicationInputFeatureFlagReader(replicationInput)
    Assertions.assertEquals(ShouldFailSyncOnDestinationTimeout.default, reader.read(ShouldFailSyncOnDestinationTimeout))
  }
}
