/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DeploymentMetadataRead
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs.AirbyteEdition
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import java.util.UUID

@Singleton
open class DeploymentMetadataHandler(
  private val airbyteVersion: AirbyteVersion,
  private val airbyteEdition: AirbyteEdition,
  @param:Named("unwrappedConfig") private val dslContext: DSLContext,
) {
  fun getDeploymentMetadata(): DeploymentMetadataRead {
    val result = dslContext.fetch(DEPLOYMENT_ID_QUERY)
    val deploymentId = result.getValue(0, "value").toString()
    return DeploymentMetadataRead()
      .id(UUID.fromString(deploymentId))
      .mode(airbyteEdition.name)
      .version(airbyteVersion.serialize())
  }

  companion object {
    private const val DEPLOYMENT_ID_QUERY = "SELECT value FROM airbyte_metadata where key = 'deployment_id'"
  }
}
