/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package featureflag

import io.airbyte.featureflag.ContextAppender
import io.airbyte.featureflag.ContextInterceptor
import io.airbyte.featureflag.Dataplane
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.LaunchDarklyClient
import io.airbyte.workload.launcher.featureflag.FeatureFlagContextUpdater
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class FeatureFlagContextUpdaterTest {
  @Test
  fun `verify it registers an interceptor when a dataplane config is published`() {
    val contextInterceptorCaptor = slot<ContextInterceptor>()
    val client: LaunchDarklyClient =
      mockk(relaxed = true) {
        every { registerContextInterceptor(capture(contextInterceptorCaptor)) } just runs
      }
    val updater = FeatureFlagContextUpdater(client)

    val event =
      DataplaneConfig(
        dataplaneId = UUID.randomUUID(),
        dataplaneName = "my-dataplane",
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "my-dataplane-group",
        dataplaneEnabled = false,
      )
    updater.onApplicationEvent(event)

    val expectedContextInterceptor =
      ContextAppender(
        contexts =
          listOf(
            Dataplane(event.dataplaneId),
            DataplaneGroup(event.dataplaneGroupId),
          ),
      )
    assertEquals(expectedContextInterceptor, contextInterceptorCaptor.captured)
  }
}
