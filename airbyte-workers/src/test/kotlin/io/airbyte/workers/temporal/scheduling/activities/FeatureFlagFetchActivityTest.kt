/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.featureflag.EnforceDataWorkerCapacity
import io.airbyte.featureflag.TestClient
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class FeatureFlagFetchActivityTest {
  private var mAirbyteApiClient: AirbyteApiClient? = null
  private var mWorkspaceApi: WorkspaceApi? = null
  private var featureFlagFetchActivity: FeatureFlagFetchActivity? = null
  private val organizationId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    mWorkspaceApi = mockk<WorkspaceApi>()
    mAirbyteApiClient = mockk<AirbyteApiClient>()
    every { mAirbyteApiClient!!.workspaceApi } returns mWorkspaceApi!!
    every { mWorkspaceApi!!.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>()) } returns
      WorkspaceRead(
        UUID.randomUUID(),
        organizationId,
        "",
        "",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
    featureFlagFetchActivity =
      FeatureFlagFetchActivityImpl(
        mAirbyteApiClient!!,
        TestClient(mapOf(EnforceDataWorkerCapacity.key to true)),
      )
  }

  @Test
  fun testGetFeatureFlags() {
    val input = FeatureFlagFetchInput(CONNECTION_ID)

    val featureFlagFetchOutput =
      featureFlagFetchActivity!!.getFeatureFlags(input)

    assertEquals(true, featureFlagFetchOutput.featureFlags!![EnforceDataWorkerCapacity.key])
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
  }
}
