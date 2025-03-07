/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object SourceDefinitionSpecificationReadMapper {
  /**
   * Converts a SourceDefinitionRead object from the config api to an object without including job
   * info.
   *
   * @param sourceDefinitionSpecificationRead Output of a connection create/get from config api
   * @return SourceDefinitionSpecificationRead Response object with everything except jobInfo
   */
  fun from(sourceDefinitionSpecificationRead: SourceDefinitionSpecificationRead): SourceDefinitionSpecificationRead {
    sourceDefinitionSpecificationRead.jobInfo = null
    return sourceDefinitionSpecificationRead
  }
}
