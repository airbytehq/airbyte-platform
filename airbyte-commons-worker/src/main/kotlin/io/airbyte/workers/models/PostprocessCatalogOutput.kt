package io.airbyte.workers.models

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.CatalogDiff

/**
 * A very basic discriminated union of a successful catalog postprocess and an error. Allows bypassing
 * extraneous exception wrapping / propagation. Written naively to allow interop with Java.
 */
@JsonDeserialize(builder = PostprocessCatalogOutput.Builder::class)
data class PostprocessCatalogOutput private constructor(val diff: CatalogDiff?, val message: String?, val stackTrace: String?) {
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Builder
    @JvmOverloads
    constructor(var diff: CatalogDiff? = null, var message: String? = null, var stackTrace: String? = null) {
      fun diff(diff: CatalogDiff) = apply { this.diff = diff }

      fun message(message: String) = apply { this.message = message }

      fun stackTrace(stackTrace: String) = apply { this.stackTrace = stackTrace }

      fun build() = PostprocessCatalogOutput(diff, message, stackTrace)
    }

  @JsonIgnore
  val isSuccess = message == null && stackTrace == null

  @JsonIgnore
  val isFailure = !isSuccess

  companion object {
    fun success(diff: CatalogDiff?): PostprocessCatalogOutput = PostprocessCatalogOutput(diff, null, null)

    fun failure(t: Throwable): PostprocessCatalogOutput = PostprocessCatalogOutput(null, t.message, t.stackTraceToString())
  }
}
