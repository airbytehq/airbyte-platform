/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.SynchronousJobRead

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object SourceDefinitionSpecificationReadMapper {
  /**
   * Converts a SourceDefinitionRead object from the config api to an object without including job
   * info.
   *
   * @param sourceDefinitionSpecificationRead Output of a connection create/get from config api
   * @return SourceDefinitionSpecificationRead Response object with everything except jobInfo's logs
   */
  fun from(sourceDefinitionSpecificationRead: SourceDefinitionSpecificationRead): SourceDefinitionSpecificationRead {
    return SourceDefinitionSpecificationRead(
      sourceDefinitionId = sourceDefinitionSpecificationRead.sourceDefinitionId,
      documentationUrl = sourceDefinitionSpecificationRead.documentationUrl,
      connectionSpecification = sourceDefinitionSpecificationRead.connectionSpecification,
      advancedAuth = sourceDefinitionSpecificationRead.advancedAuth,
      jobInfo =
        SynchronousJobRead(
          id = sourceDefinitionSpecificationRead.jobInfo.id,
          configType = sourceDefinitionSpecificationRead.jobInfo.configType,
          createdAt = sourceDefinitionSpecificationRead.jobInfo.createdAt,
          endedAt = sourceDefinitionSpecificationRead.jobInfo.endedAt,
          succeeded = sourceDefinitionSpecificationRead.jobInfo.succeeded,
          configId = sourceDefinitionSpecificationRead.jobInfo.configId,
          connectorConfigurationUpdated = sourceDefinitionSpecificationRead.jobInfo.connectorConfigurationUpdated,
          logs = null,
          failureReason = sourceDefinitionSpecificationRead.jobInfo.failureReason,
        ),
    )
  }
}
