/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.input

import io.airbyte.initContainer.hydration.CheckConnectionInputHydrator
import io.airbyte.initContainer.serde.ObjectSerializer
import io.airbyte.initContainer.system.FileClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags.CONNECTION_ID
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_IMAGE
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_TYPE
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import secrets.persistence.SecretCoordinateException

@Requires(property = "airbyte.init.operation", pattern = "check")
@Singleton
class CheckHydrationProcessor(
  private val inputHydrator: CheckConnectionInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
  private val metricClient: MetricClient,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    val rawPayload = workload.inputPayload
    val parsed: CheckConnectionInput = deserializer.toCheckConnectionInput(rawPayload)

    val hydrated =
      try {
        inputHydrator.getHydratedStandardCheckInput(parsed.checkConnectionInput)
      } catch (e: SecretCoordinateException) {
        val attrs =
          mutableListOf(
            MetricAttribute(CONNECTOR_IMAGE, parsed.launcherConfig.dockerImage),
            MetricAttribute(
              CONNECTOR_TYPE,
              parsed.checkConnectionInput.actorContext.actorType
                .toString(),
            ),
          )
        if (parsed.launcherConfig.connectionId != null) {
          attrs.add(MetricAttribute(CONNECTION_ID, parsed.launcherConfig.connectionId.toString()))
        }

        metricClient.count(
          metric = OssMetricsRegistry.SECRETS_HYDRATION_FAILURE,
          attributes = attrs.toTypedArray(),
        )
        throw e
      }

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
