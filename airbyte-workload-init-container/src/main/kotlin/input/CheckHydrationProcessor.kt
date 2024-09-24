package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = "airbyte.init.operation", pattern = "check")
@Singleton
class CheckHydrationProcessor(
  private val inputHydrator: CheckConnectionInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    val rawPayload = workload.inputPayload
    val parsed: CheckConnectionInput = deserializer.toCheckConnectionInput(rawPayload)

    val hydrated = inputHydrator.getHydratedStandardCheckInput(parsed.checkConnectionInput)

    fileClient.writeInputFile(
      FileConstants.CONNECTION_CONFIGURATION_FILE,
      serializer.serialize(hydrated.connectionConfiguration),
    )

    fileClient.writeInputFile(
      FileConstants.SIDECAR_INPUT_FILE,
      serializer.serialize(
        SidecarInput(
          hydrated,
          null,
          workload.id,
          parsed.launcherConfig,
          SidecarInput.OperationType.CHECK,
          workload.logPath,
        ),
      ),
    )
  }
}
