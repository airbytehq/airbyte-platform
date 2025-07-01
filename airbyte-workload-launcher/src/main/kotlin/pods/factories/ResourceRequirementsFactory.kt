/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.config.helpers.ResourceRequirementsUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.getDestinationResourceReqs
import io.airbyte.workers.input.getOrchestratorResourceReqs
import io.airbyte.workers.input.getSourceResourceReqs
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
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
  fun orchestrator(input: ReplicationInput): AirbyteResourceRequirements? = input.getOrchestratorResourceReqs()

  fun replSource(input: ReplicationInput): AirbyteResourceRequirements? {
    val sourceReqs =
      if (input.useFileTransfer) {
        input.getSourceResourceReqs()?.let {
          ResourceRequirementsUtils.mergeResourceRequirements(it, fileTransferReqs)
        } ?: fileTransferReqs
      } else {
        input.getSourceResourceReqs()
      }
    return sourceReqs
  }

  fun replDestination(input: ReplicationInput): AirbyteResourceRequirements? = input.getDestinationResourceReqs()

  fun sidecar(): AirbyteResourceRequirements = sidecarReqs

  fun checkConnector(input: CheckConnectionInput): AirbyteResourceRequirements =
    input.checkConnectionInput.resourceRequirements?.let {
      ResourceRequirementsUtils.mergeResourceRequirements(it, checkConnectorReqs)
    } ?: checkConnectorReqs

  fun discoverConnector(input: DiscoverCatalogInput): AirbyteResourceRequirements =
    input.discoverCatalogInput.resourceRequirements?.let {
      ResourceRequirementsUtils.mergeResourceRequirements(it, discoverConnectorReqs)
    } ?: discoverConnectorReqs

  fun specConnector(): AirbyteResourceRequirements = specConnectorReqs

  fun replInit(input: ReplicationInput): AirbyteResourceRequirements? = input.getOrchestratorResourceReqs()

  fun checkInit(input: CheckConnectionInput): AirbyteResourceRequirements {
    val connectorReqs = checkConnector(input)
    return ResourceConversionUtils.sumResourceRequirements(connectorReqs, sidecarReqs)
  }

  fun discoverInit(input: DiscoverCatalogInput): AirbyteResourceRequirements {
    val connectorReqs = discoverConnector(input)
    return ResourceConversionUtils.sumResourceRequirements(connectorReqs, sidecarReqs)
  }

  fun specInit(): AirbyteResourceRequirements = specConnectorReqs
}
