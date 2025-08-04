/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.tracing

import datadog.trace.api.interceptor.MutableSpan

internal class DummySpan : MutableSpan {
  private val tags: MutableMap<String?, Any?> = HashMap<String?, Any?>()
  private var error = false
  private var operationName: String? = null
  private var resourceName: String? = null

  override fun getStartTime(): Long = 0

  override fun getDurationNano(): Long = 0

  override fun getOperationName(): CharSequence? = operationName

  override fun setOperationName(operationName: CharSequence?): MutableSpan {
    this.operationName = if (operationName != null) operationName.toString() else null
    return this
  }

  override fun getServiceName(): String? = null

  override fun setServiceName(serviceName: String?): MutableSpan? = null

  override fun getResourceName(): CharSequence? = resourceName

  override fun setResourceName(resourceName: CharSequence?): MutableSpan {
    this.resourceName = if (resourceName != null) resourceName.toString() else null
    return this
  }

  override fun getSamplingPriority(): Int? = null

  override fun setSamplingPriority(newPriority: Int): MutableSpan? = null

  override fun getSpanType(): String? = null

  override fun setSpanType(type: CharSequence?): MutableSpan? = null

  override fun getTags(): MutableMap<String?, Any?> = tags

  override fun setTag(
    tag: String?,
    value: String?,
  ): MutableSpan {
    tags.put(tag, value)
    return this
  }

  override fun setTag(
    tag: String?,
    value: Boolean,
  ): MutableSpan {
    tags.put(tag, value)
    return this
  }

  override fun setTag(
    tag: String?,
    value: Number?,
  ): MutableSpan {
    tags.put(tag, value)
    return this
  }

  override fun setMetric(
    metric: CharSequence?,
    value: Int,
  ): MutableSpan? = null

  override fun setMetric(
    metric: CharSequence?,
    value: Long,
  ): MutableSpan? = null

  override fun setMetric(
    metric: CharSequence?,
    value: Double,
  ): MutableSpan? = null

  override fun isError(): Boolean = error

  override fun setError(value: Boolean): MutableSpan {
    error = value
    return this
  }

  override fun getRootSpan(): MutableSpan? = null

  override fun getLocalRootSpan(): MutableSpan? = null
}
