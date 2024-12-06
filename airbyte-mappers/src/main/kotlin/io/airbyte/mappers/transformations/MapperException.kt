package io.airbyte.mappers.transformations

open class MapperException(val type: DestinationCatalogGenerator.MapperErrorType, message: String, cause: Throwable? = null) : RuntimeException(
  message,
  cause,
)
