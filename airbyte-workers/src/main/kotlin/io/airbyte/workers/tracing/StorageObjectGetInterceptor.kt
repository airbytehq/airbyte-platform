/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing

import datadog.trace.api.interceptor.MutableSpan
import datadog.trace.api.interceptor.TraceInterceptor

/**
 * Intercept traces to mark erroneous errors as non errors.
 */
class StorageObjectGetInterceptor : TraceInterceptor {
  override fun onTraceComplete(trace: Collection<MutableSpan>): Collection<MutableSpan> {
    val filtered = mutableListOf<MutableSpan>()
    trace.forEach { s: MutableSpan ->
      val tags = s.tags
      // if no tags, then keep the span and move on to the next one
      if (tags == null || tags.isEmpty()) {
        filtered.add(s)
        return@forEach
      }

      // There are two different errors spans that we want to ignore, both of which are specific to
      // "storage.googleapis.com". One returns a http status code of 404 and the other has an error
      // message
      // that begins with "404 Not Found"
      val is404 =
        tags.getOrDefault("http.status_code", "") == 404 ||
          (tags.getOrDefault("error.msg", "") as String).startsWith("404 Not Found")
      if (s.isError &&
        "storage.googleapis.com" == tags.getOrDefault("peer.hostname", "") &&
        is404
      ) {
        // Mark these spans as non-errors as this is expected behavior based on our
        // current google storage usage.
        s.setError(false)
      }
      filtered.add(s)
    }

    return filtered
  }

  override fun priority(): Int = 404
}
