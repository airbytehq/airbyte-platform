/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.input

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadType
import io.airbyte.initContainer.hydration.DiscoverCatalogInputHydrator
import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.domain.Workload
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class DiscoverHydrationProcessorTest {
  @MockK
  lateinit var inputHydrator: DiscoverCatalogInputHydrator

  @MockK
  lateinit var serializer: ObjectSerializer

  @MockK
  lateinit var deserializer: PayloadDeserializer

  @MockK
  lateinit var fileClient: FileClient

  @MockK(relaxed = true)
  lateinit var metricClient: MetricClient

  private lateinit var processor: DiscoverHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      DiscoverHydrationProcessor(
        inputHydrator,
        deserializer,
        serializer,
        fileClient,
        metricClient,
      )
  }

  @Test
  fun `parses input, hydrates and writes output to expected file`() {
    val input = Fixtures.workload

    val unhydrated = StandardDiscoverCatalogInput()
    val parsed =
      DiscoverCatalogInput(
        jobRunConfig = JobRunConfig(),
        discoverCatalogInput = unhydrated,
        launcherConfig = IntegrationLauncherConfig(),
      )

    val connectionConfiguration = Jsons.jsonNode(mapOf("key-1" to "value-1"))
    val hydrated = StandardDiscoverCatalogInput()
    hydrated.connectionConfiguration = connectionConfiguration
    val serializedConfig = "serialized hydrated config"
    val serializedInput = "serialized hydrated blob"

    val sidecarInput =
      SidecarInput(
        null,
        hydrated,
        input.id,
        parsed.launcherConfig,
        SidecarInput.OperationType.DISCOVER,
        input.logPath,
      )

    every { deserializer.toDiscoverCatalogInput(input.inputPayload) } returns parsed
    every { inputHydrator.getHydratedStandardDiscoverInput(unhydrated) } returns hydrated
    every { serializer.serialize(connectionConfiguration) } returns serializedConfig
    every { serializer.serialize(sidecarInput) } returns serializedInput
    every { fileClient.writeInputFile(FileConstants.CONNECTION_CONFIGURATION_FILE, serializedConfig) } returns Unit
    every { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) } returns Unit

    processor.process(input)

    verify { deserializer.toDiscoverCatalogInput(input.inputPayload) }
    verify { inputHydrator.getHydratedStandardDiscoverInput(unhydrated) }
    verify { serializer.serialize(connectionConfiguration) }
    verify { serializer.serialize(sidecarInput) }
    verify { fileClient.writeInputFile(FileConstants.CONNECTION_CONFIGURATION_FILE, serializedConfig) }
    verify { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) }
  }

  object Fixtures {
    private const val WORKLOAD_ID = "workload-id-15"

    val workload =
      Workload(
        id = WORKLOAD_ID,
        labels = mutableListOf(),
        inputPayload = "inputPayload",
        logPath = "logPath",
        type = WorkloadType.DISCOVER,
        autoId = UUID.randomUUID(),
      )
  }
}
