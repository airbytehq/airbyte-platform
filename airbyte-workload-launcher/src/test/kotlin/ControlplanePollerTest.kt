/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.client.model.generated.DataplaneHeartbeatResponse
import io.airbyte.api.client.model.generated.DataplaneInitRequestBody
import io.airbyte.api.client.model.generated.DataplaneInitResponse
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.WorkloadLauncherUseDataPlaneAuthNFlow
import io.airbyte.workload.launcher.ControlplanePoller
import io.airbyte.workload.launcher.config.DataplaneCredentials
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micronaut.context.event.ApplicationEventPublisher
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

class ControlplanePollerTest {
  val dataplaneName = "test-dataplane"
  val dataplaneCreds =
    DataplaneCredentials(
      clientId = dataplaneName,
      clientSecret = "some-secret-secret",
    )
  lateinit var apiClient: AirbyteApiClient
  lateinit var featureFlagMap: MutableMap<String, Any>
  lateinit var featureFlagClient: FeatureFlagClient
  lateinit var poller: ControlplanePoller
  lateinit var eventPublisher: ApplicationEventPublisher<DataplaneConfig>

  @BeforeEach
  fun setup() {
    apiClient = mockk()
    featureFlagMap =
      mutableMapOf(
        WorkloadLauncherUseDataPlaneAuthNFlow.key to true,
      )
    featureFlagClient = TestClient(featureFlagMap)
    eventPublisher = mockk(relaxed = true)
    poller =
      ControlplanePoller(
        dataplaneName = dataplaneName,
        dataplaneCredentials = dataplaneCreds,
        airbyteApiClient = apiClient,
        featureFlagClient = featureFlagClient,
        eventPublisher = eventPublisher,
      )
  }

  @Test
  fun `Poller should phone home on initialize`() {
    val initResponse =
      DataplaneInitResponse(
        dataplaneName = "dataplane-name",
        dataplaneId = UUID.randomUUID(),
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "group-name",
      )
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { initializeDataplane(DataplaneInitRequestBody(dataplaneCreds.clientId)) } returns initResponse
      }

    poller.initialize()
    val expectedConfig =
      DataplaneConfig(
        dataplaneName = initResponse.dataplaneName,
        dataplaneId = initResponse.dataplaneId,
        dataplaneEnabled = initResponse.dataplaneEnabled,
        dataplaneGroupId = initResponse.dataplaneGroupId,
        dataplaneGroupName = initResponse.dataplaneGroupName,
      )
    assertEquals(expectedConfig, poller.dataplaneConfig)

    verify { eventPublisher.publishEvent(expectedConfig) }
  }

  @Test
  fun `Poller should throw on failed initialize call`() {
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { initializeDataplane(DataplaneInitRequestBody(dataplaneCreds.clientId)) } throws ClientException("Forbidden", 403)
      }

    assertThrows<Exception> {
      poller.initialize()
    }
    assertNull(poller.dataplaneConfig)
    verify(exactly = 0) { eventPublisher.publishEvent(any()) }
  }

  @Test
  fun `Poller shouldn't poll if feature flag is off`() {
    featureFlagMap[WorkloadLauncherUseDataPlaneAuthNFlow.key] = false
    poller.initialize()
    verify(exactly = 0) {
      apiClient.dataplaneApi.initializeDataplane(any())
    }
    assertNull(poller.dataplaneConfig)
    verify(exactly = 0) { eventPublisher.publishEvent(any()) }
  }

  @Test
  fun `Heartbeat should publish changes`() {
    val heartbeatResponse =
      DataplaneHeartbeatResponse(
        dataplaneName = "dataplane-name",
        dataplaneId = UUID.randomUUID(),
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "group-name",
      )
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { heartbeatDataplane(DataplaneHeartbeatRequestBody(dataplaneCreds.clientId)) } returns heartbeatResponse
      }

    poller.heartbeat()

    val expectedConfig =
      DataplaneConfig(
        dataplaneName = heartbeatResponse.dataplaneName,
        dataplaneId = heartbeatResponse.dataplaneId,
        dataplaneEnabled = heartbeatResponse.dataplaneEnabled,
        dataplaneGroupId = heartbeatResponse.dataplaneGroupId,
        dataplaneGroupName = heartbeatResponse.dataplaneGroupName,
      )
    verify { eventPublisher.publishEvent(expectedConfig) }
  }

  @Test
  fun `Heartbeat should only publish changes and omit duplicates`() {
    val heartbeatResponse =
      DataplaneHeartbeatResponse(
        dataplaneName = "dataplane-name",
        dataplaneId = UUID.randomUUID(),
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "group-name",
      )
    val heartbeatResponse2 = heartbeatResponse.copy(dataplaneId = UUID.randomUUID())
    val heartbeatResponse3 = heartbeatResponse.copy(dataplaneId = UUID.randomUUID())
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every {
          heartbeatDataplane(
            DataplaneHeartbeatRequestBody(dataplaneCreds.clientId),
          )
        } returns heartbeatResponse andThen
          heartbeatResponse andThen
          heartbeatResponse2 andThen
          heartbeatResponse3 andThen
          heartbeatResponse3 andThen
          heartbeatResponse3
      }

    repeat(6) {
      poller.heartbeat()
    }

    verify(exactly = 1) {
      eventPublisher.publishEvent(heartbeatResponse.toConfig())
      eventPublisher.publishEvent(heartbeatResponse2.toConfig())
      eventPublisher.publishEvent(heartbeatResponse3.toConfig())
    }
  }

  @Test
  fun `Heartbeat is a noop if feature flag is off`() {
    featureFlagMap[WorkloadLauncherUseDataPlaneAuthNFlow.key] = false

    poller.heartbeat()
    verify(exactly = 0) {
      apiClient.dataplaneApi.heartbeatDataplane(any())
      eventPublisher.publishEvent(any())
    }
  }

  private fun DataplaneHeartbeatResponse.toConfig(): DataplaneConfig =
    DataplaneConfig(
      dataplaneName = dataplaneName,
      dataplaneId = dataplaneId,
      dataplaneEnabled = dataplaneEnabled,
      dataplaneGroupId = dataplaneGroupId,
      dataplaneGroupName = dataplaneGroupName,
    )
}
