package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags.CONNECTION_ID
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_IMAGE
import io.airbyte.metrics.lib.MetricTags.CONNECTOR_TYPE
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import secrets.persistence.SecretCoordinateException

@Requires(property = "airbyte.init.operation", pattern = "discover")
@Singleton
class DiscoverHydrationProcessor(
  private val inputHydrator: DiscoverCatalogInputHydrator,
  private val deserializer: PayloadDeserializer,
  private val serializer: ObjectSerializer,
  private val fileClient: FileClient,
  private val metricClient: MetricClient,
) : InputHydrationProcessor {
  override fun process(workload: Workload) {
    val rawPayload = workload.inputPayload
    val parsed: DiscoverCatalogInput = deserializer.toDiscoverCatalogInput(rawPayload)

    val hydrated =
      try {
        inputHydrator.getHydratedStandardDiscoverInput(parsed.discoverCatalogInput)
      } catch (e: SecretCoordinateException) {
        val attrs =
          listOf(
            MetricAttribute(CONNECTOR_IMAGE, parsed.launcherConfig.dockerImage),
            MetricAttribute(CONNECTOR_TYPE, parsed.discoverCatalogInput.actorContext.actorType.toString()),
            MetricAttribute(CONNECTION_ID, parsed.launcherConfig.connectionId.toString()),
          )
        metricClient.count(OssMetricsRegistry.SECRETS_HYDRATION_FAILURE, 1, *attrs.toTypedArray())
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
          null,
          hydrated,
          workload.id,
          parsed.launcherConfig,
          SidecarInput.OperationType.DISCOVER,
          workload.logPath,
        ),
      ),
    )
  }
}
