package io.airbyte.workers.models

import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead

/**
 * A very basic discriminated union of a successful catalog postprocess and an error. Allows bypassing
 * extraneous exception wrapping / propagation. Written naively to allow interop with Java.
 */
data class PostprocessCatalogOutput private constructor(val discoverRead: SourceDiscoverSchemaRead?, val error: Throwable?) {
  val isSuccess = error == null
  val isFailure = error != null

  companion object {
    fun success(discoverRead: SourceDiscoverSchemaRead): PostprocessCatalogOutput = PostprocessCatalogOutput(discoverRead, null)

    fun failure(t: Throwable): PostprocessCatalogOutput = PostprocessCatalogOutput(null, t)
  }
}
