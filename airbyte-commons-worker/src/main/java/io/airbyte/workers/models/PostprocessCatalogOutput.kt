package io.airbyte.workers.models

import io.airbyte.config.CatalogDiff

/**
 * A very basic discriminated union of a successful catalog postprocess and an error. Allows bypassing
 * extraneous exception wrapping / propagation. Written naively to allow interop with Java.
 */
data class PostprocessCatalogOutput private constructor(val diff: CatalogDiff?, val message: String?, val stackTrace: String?) {
  val isSuccess = message == null && stackTrace == null
  val isFailure = !isSuccess

  companion object {
    fun success(diff: CatalogDiff?): PostprocessCatalogOutput = PostprocessCatalogOutput(diff, null, null)

    fun failure(t: Throwable): PostprocessCatalogOutput = PostprocessCatalogOutput(null, t.message, t.stackTraceToString())
  }
}
