package io.airbyte.workers.general

import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseStreamStatusTracker2024
import io.airbyte.featureflag.Workspace
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class CachingFeatureFlagClientTest {
  private lateinit var client: CachingFeatureFlagClient
  private lateinit var rawClient: TestClient

  @BeforeEach
  fun setup() {
    rawClient = mockk()
    client = CachingFeatureFlagClient(rawClient)
  }

  @Test
  fun cachesTheValueForBooleans() {
    val context = Workspace(UUID.randomUUID())

    every { rawClient.boolVariation(any(), any()) } returns true
    val result1 = client.boolVariation(UseStreamStatusTracker2024, context)
    Assertions.assertTrue(result1)

    every { rawClient.boolVariation(any(), any()) } returns false
    val result2 = client.boolVariation(UseStreamStatusTracker2024, context)
    Assertions.assertTrue(result2)

    every { rawClient.boolVariation(any(), any()) } returns false
    val result3 = client.boolVariation(UseStreamStatusTracker2024, context)
    Assertions.assertTrue(result3)
  }
}
