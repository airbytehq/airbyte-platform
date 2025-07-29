/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.authn

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DataplaneHeartbeatRequestBody
import io.airbyte.api.client.model.generated.DataplaneHeartbeatResponse
import io.airbyte.api.client.model.generated.DataplaneInitRequestBody
import io.airbyte.api.client.model.generated.DataplaneInitResponse
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micronaut.context.event.ApplicationEventPublisher
import io.mockk.Ordering
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

class DataplaneIdentityServiceTest {
  val dataplaneClientId = "test-dp-client-id-1"

  lateinit var apiClient: AirbyteApiClient
  lateinit var featureFlagMap: MutableMap<String, Any>
  lateinit var featureFlagClient: FeatureFlagClient
  lateinit var eventPublisher: ApplicationEventPublisher<DataplaneConfig>
  lateinit var service: DataplaneIdentityService

  @BeforeEach
  fun setup() {
    apiClient = mockk()
    featureFlagMap = mutableMapOf()
    featureFlagClient = TestClient(featureFlagMap)
    eventPublisher = mockk(relaxed = true)
    service =
      DataplaneIdentityService(
        dataplaneClientId = dataplaneClientId,
        airbyteApiClient = apiClient,
        eventPublisher = eventPublisher,
      )
  }

  @Test
  fun `phones home on initialize`() {
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
        every { initializeDataplane(DataplaneInitRequestBody(dataplaneClientId)) } returns initResponse
      }

    service.initialize()
    val expectedConfig =
      DataplaneConfig(
        dataplaneName = initResponse.dataplaneName,
        dataplaneId = initResponse.dataplaneId,
        dataplaneEnabled = initResponse.dataplaneEnabled,
        dataplaneGroupId = initResponse.dataplaneGroupId,
        dataplaneGroupName = initResponse.dataplaneGroupName,
      )
    assertEquals(expectedConfig, service.authNDrivenDataplaneConfig)

    verify { eventPublisher.publishEvent(expectedConfig) }
  }

  @Test
  fun `throws on failed initialize call`() {
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { initializeDataplane(DataplaneInitRequestBody(dataplaneClientId)) } throws ClientException("Forbidden", 403)
      }

    assertThrows<Exception> {
      service.initialize()
    }
    assertNull(service.authNDrivenDataplaneConfig)
    verify(exactly = 0) { eventPublisher.publishEvent(any()) }
  }

  @Test
  fun `Heartbeat should publish changes`() {
    val heartbeatResponse = defaultHeartbeatResponse
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { heartbeatDataplane(DataplaneHeartbeatRequestBody(dataplaneClientId)) } returns heartbeatResponse
      }

    service.heartbeat()

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
    val heartbeatResponse = defaultHeartbeatResponse
    val heartbeatResponse2 = heartbeatResponse.copy(dataplaneId = UUID.randomUUID())
    val heartbeatResponse3 = heartbeatResponse.copy(dataplaneId = UUID.randomUUID())
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every {
          heartbeatDataplane(
            DataplaneHeartbeatRequestBody(dataplaneClientId),
          )
        } returns heartbeatResponse andThen
          heartbeatResponse andThen
          heartbeatResponse2 andThen
          heartbeatResponse3 andThen
          heartbeatResponse3 andThen
          heartbeatResponse3
      }

    repeat(6) {
      service.heartbeat()
    }

    verify(exactly = 1) {
      eventPublisher.publishEvent(heartbeatResponse.toConfig())
      eventPublisher.publishEvent(heartbeatResponse2.toConfig())
      eventPublisher.publishEvent(heartbeatResponse3.toConfig())
    }
  }

  @Test
  fun `Heartbeat should send publish a disable config on 401`() {
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { heartbeatDataplane(any()) } returns defaultHeartbeatResponse andThenThrows ClientException(statusCode = 401)
      }
    service.heartbeat()
    service.heartbeat()
    verify(ordering = Ordering.ORDERED) {
      eventPublisher.publishEvent(defaultHeartbeatResponse.toConfig())
      eventPublisher.publishEvent(match { !it.dataplaneEnabled })
    }
  }

  @Test
  fun `Heartbeat should send publish a disable config on 403`() {
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { heartbeatDataplane(any()) } returns defaultHeartbeatResponse andThenThrows ClientException(statusCode = 403)
      }
    service.heartbeat()
    service.heartbeat()
    verify(ordering = Ordering.ORDERED) {
      eventPublisher.publishEvent(defaultHeartbeatResponse.toConfig())
      eventPublisher.publishEvent(match { !it.dataplaneEnabled })
    }
  }

  @Test
  fun `getters returns api config based values`() {
    val initResponse =
      DataplaneInitResponse(
        dataplaneName = "data-driven-dataplane-name",
        dataplaneId = UUID.randomUUID(),
        dataplaneEnabled = true,
        dataplaneGroupId = UUID.randomUUID(),
        dataplaneGroupName = "data-driven-dataplane-group-name",
      )
    every {
      apiClient.dataplaneApi
    } returns
      mockk {
        every { initializeDataplane(DataplaneInitRequestBody(dataplaneClientId)) } returns initResponse
      }
    service.initialize()

    assertEquals(initResponse.dataplaneName, service.getDataplaneName())
    assertEquals(initResponse.dataplaneId.toString(), service.getDataplaneId())
  }

  private val defaultHeartbeatResponse =
    DataplaneHeartbeatResponse(
      dataplaneName = "dataplane-name",
      dataplaneId = UUID.randomUUID(),
      dataplaneEnabled = true,
      dataplaneGroupId = UUID.randomUUID(),
      dataplaneGroupName = "group-name",
    )

  private fun DataplaneHeartbeatResponse.toConfig(): DataplaneConfig =
    DataplaneConfig(
      dataplaneName = dataplaneName,
      dataplaneId = dataplaneId,
      dataplaneEnabled = dataplaneEnabled,
      dataplaneGroupId = dataplaneGroupId,
      dataplaneGroupName = dataplaneGroupName,
    )
}
