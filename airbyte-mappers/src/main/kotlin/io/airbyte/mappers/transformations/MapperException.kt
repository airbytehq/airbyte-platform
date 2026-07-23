/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

open class MapperException(
  val type: DestinationCatalogGenerator.MapperErrorType,
  message: String,
  cause: Throwable? = null,
) : RuntimeException(
    message,
    cause,
  )
