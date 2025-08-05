/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.featureflag.TestClient
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.UUID

internal class FeatureFlagFetchActivityTest {
  private var mAirbyteApiClient: AirbyteApiClient? = null
  private var mWorkspaceApi: WorkspaceApi? = null
  private var mTestClient: TestClient? = null
  private var featureFlagFetchActivity: FeatureFlagFetchActivity? = null

  @BeforeEach
  @Throws(IOException::class)
  fun setUp() {
    mWorkspaceApi = org.mockito.Mockito.mock<WorkspaceApi>(WorkspaceApi::class.java)
    mAirbyteApiClient = org.mockito.Mockito.mock<AirbyteApiClient>(AirbyteApiClient::class.java)
    whenever(mAirbyteApiClient!!.workspaceApi).thenReturn(mWorkspaceApi)
    whenever(mWorkspaceApi!!.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>())).thenReturn(
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
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
      ),
    )
    mTestClient = org.mockito.Mockito.mock<TestClient>(TestClient::class.java)
    featureFlagFetchActivity = FeatureFlagFetchActivityImpl(mAirbyteApiClient!!, mTestClient!!)
  }

  @Test
  fun testGetFeatureFlags() {
    val input = FeatureFlagFetchInput(CONNECTION_ID)

    val featureFlagFetchOutput =
      featureFlagFetchActivity!!.getFeatureFlags(input)

//    Left as a sample assertion for when we have a flag to add for the ConnectionManagerWorkflow
//    Assertions.assertTrue(featureFlagFetchOutput.featureFlags!!.get(UseSyncV2.key)!!)
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
  }
}
