/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.input

import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = "airbyte.init.operation", pattern = "spec")
@Singleton
class SpecHydrationProcessor(
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    val rawPayload = workload.inputPayload
    val parsed: SpecInput = deserializer.toSpecInput(rawPayload)

    fileClient.writeInputFile(
      FileConstants.SIDECAR_INPUT_FILE,
      serializer.serialize(
        SidecarInput(
          null,
          null,
          workload.id,
          parsed.launcherConfig,
          SidecarInput.OperationType.SPEC,
          workload.logPath,
        ),
      ),
    )
  }
}
