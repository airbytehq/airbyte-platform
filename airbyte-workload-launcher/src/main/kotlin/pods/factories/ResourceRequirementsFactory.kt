package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.helpers.ResourceRequirementsUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getDestinationResourceReqs
import io.airbyte.workers.input.getOrchestratorResourceReqs
import io.airbyte.workers.input.getSourceResourceReqs
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.pod.ResourceConversionUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

/**
 * Builds the resource reqs for the various containers based on runtime and static config.
 */
@Singleton
class ResourceRequirementsFactory(
  @Named("checkConnectorReqs") private val checkConnectorReqs: AirbyteResourceRequirements,
  @Named("discoverConnectorReqs") private val discoverConnectorReqs: AirbyteResourceRequirements,
  @Named("specConnectorReqs") private val specConnectorReqs: AirbyteResourceRequirements,
  @Named("sidecarReqs") private val sidecarReqs: AirbyteResourceRequirements,
  @Named("fileTransferReqs") private val fileTransferReqs: AirbyteResourceRequirements,
) {
  fun orchestrator(input: ReplicationInput): AirbyteResourceRequirements? {
    return input.getOrchestratorResourceReqs()
  }

  fun replSource(input: ReplicationInput): AirbyteResourceRequirements? {
    val sourceReqs =
      if (input.useFileTransfer) {
        input.getSourceResourceReqs()?.let {
          ResourceRequirementsUtils.mergeResourceRequirements(fileTransferReqs, it)
        } ?: fileTransferReqs
      } else {
        input.getSourceResourceReqs()
      }
    return sourceReqs
  }

  fun replDestination(input: ReplicationInput): AirbyteResourceRequirements? {
    return input.getDestinationResourceReqs()
  }

  fun sidecar(): AirbyteResourceRequirements {
    return sidecarReqs
  }

  fun checkConnector(input: CheckConnectionInput): AirbyteResourceRequirements {
    return input.checkConnectionInput.resourceRequirements?.let {
      ResourceRequirementsUtils.mergeResourceRequirements(it, checkConnectorReqs)
    } ?: checkConnectorReqs
  }

  fun discoverConnector(input: DiscoverCatalogInput): AirbyteResourceRequirements {
    return input.discoverCatalogInput.resourceRequirements?.let {
      ResourceRequirementsUtils.mergeResourceRequirements(it, discoverConnectorReqs)
    } ?: discoverConnectorReqs
  }

  fun specConnector(): AirbyteResourceRequirements {
    return specConnectorReqs
  }

  fun replInit(input: ReplicationInput): AirbyteResourceRequirements? {
    return input.getOrchestratorResourceReqs()
  }

  fun checkInit(input: CheckConnectionInput): AirbyteResourceRequirements {
    val connectorReqs = checkConnector(input)
    return ResourceConversionUtils.sumResourceRequirements(connectorReqs, sidecarReqs)
  }

  fun discoverInit(input: DiscoverCatalogInput): AirbyteResourceRequirements {
    val connectorReqs = discoverConnector(input)
    return ResourceConversionUtils.sumResourceRequirements(connectorReqs, sidecarReqs)
  }

  fun specInit(): AirbyteResourceRequirements {
    return specConnectorReqs
  }
}
